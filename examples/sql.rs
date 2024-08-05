use std::any::Any;
use std::collections::Bound;
use std::fmt::{Debug, Formatter};
use std::pin::{Pin, pin};
use arrow::{
    array::{ArrayRef, Float32Array, Float64Array},
    datatypes::DataType,
    record_batch::RecordBatch,
    util::pretty,
};
use datafusion::prelude::*;
use datafusion::{error::Result, physical_plan::functions::make_scalar_function};
use std::sync::Arc;
use std::task::{Context, Poll};
use arrow::datatypes::{Field, Schema, SchemaRef};
use async_stream::stream;
use async_trait::async_trait;
use datafusion::common::internal_err;
use datafusion::datasource::{MemTable, TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Volatility;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, PlanProperties};
use futures_core::Stream;
use futures_util::StreamExt;
use parquet::errors::ParquetError;
use tonbo::executor::tokio::TokioExecutor;
use tonbo::record::Record;
use tonbo::{DB, Scan};
use tonbo::inmem::immutable::ArrowArrays;
use tonbo_marco::tonbo_record;
use tonbo::transaction::Transaction;

#[tonbo_record]
pub struct Music {
    #[primary_key]
    id: u64,
    name: String,
    like: i64,
}

struct MusicProvider {
    schema: SchemaRef,
    db: DB<Music, TokioExecutor>,
}

struct MusicExec<'exec> {
    cache: PlanProperties,
    txn: Transaction<'exec, Music, TokioExecutor>,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    range: (Bound<<Music as Record>::Key>, Bound<<Music as Record>::Key>)
}

struct MusicStream<'stream> {
    stream: Pin<Box<dyn Stream<Item=Result<RecordBatch, DataFusionError>> + 'stream + Send>>
}

#[async_trait]
impl TableProvider for MusicProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Music::arrow_schema().clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(&self, state: &SessionState, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>) -> Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }
}

impl<'exec> MusicExec<'exec> {
    fn new(txn: Transaction<'exec, Music, TokioExecutor>) -> Self {
        MusicExec {
            cache: PlanProperties::new(
                EquivalenceProperties::new_with_orderings(Music::arrow_schema().clone(), &[]),
                datafusion::physical_expr::Partitioning::UnknownPartitioning(1),
                ExecutionMode::Unbounded,
            ),
            txn,
            projection: None,
            limit: None,
            range: (Bound::Unbounded, Bound::Unbounded),
        }
    }
}

impl Stream for MusicStream<'_> {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        pin!(&mut self.stream).poll_next(cx)
    }
}

impl RecordBatchStream for MusicStream<'_> {
    fn schema(&self) -> SchemaRef {
        Music::arrow_schema().clone()
    }
}

impl DisplayAs for MusicExec<'_> {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        todo!()
    }
}

impl Debug for MusicExec<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl<'exec> ExecutionPlan for MusicExec<'exec> {
    fn name(&self) -> &str {
        "MusicExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            internal_err!("Children cannot be replaced in {self:?}")
        }
    }

    fn execute(&self, _: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(MusicStream::<'exec> {
            stream: Box::pin(stream! {
                let limit = self.limit.clone();
                let projection = self.projection.clone();
                let mut scan = self.txn
                    .scan((Bound::Unbounded, Bound::Unbounded))
                    .await;
                if let Some(limit) = limit {
                    scan = scan.limit(limit);
                }
                if let Some(projection) = projection {
                    scan = scan.projection(projection.clone());
                }
                let mut scan = scan.package(8192).await.map_err(|err| DataFusionError::Internal(err.to_string()))?;

                while let Some(record) = scan.next().await {
                    yield Ok(record?.as_record_batch().clone())
                }
            }),
        }))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // define a schema.
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Float32, false),
        Field::new("b", DataType::Float64, false),
    ]));
    // define data.
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Float32Array::from(vec![2.1, 3.1, 4.1, 5.1])),
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
        ],
    )?;
    let mut ctx = SessionContext::new();
    let provider = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("t", Arc::new(provider))?;

    let pow = |args: &[ArrayRef]| {
        assert_eq!(args.len(), 2);
        let base = &args[0]
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("cast failed");
        let exponent = &args[1]
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("cast failed");
        assert_eq!(exponent.len(), base.len());

        let array = base
            .iter()
            .zip(exponent.iter())
            .map(|(base, exponent)| {
                match (base, exponent) {
                    (Some(base), Some(exponent)) => Some(base.powf(exponent)),
                    _ => None,
                }
            })
            .collect::<Float64Array>();

        Ok(Arc::new(array) as ArrayRef)
    };

    let pow = make_scalar_function(pow);
    let pow = create_udf(
        "pow",
        vec![DataType::Float64, DataType::Float64],
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        pow,
    );
    ctx.register_udf(pow.clone());

    let expr = pow.call(vec![col("a"), col("b")]);
    let df = ctx.table("t").await?;
    let pow = df.registry().udf("pow")?;
    let expr1 = pow.call(vec![col("a"), col("b")]);
    let df = df.select(vec![
        expr,
        expr1.alias("pow1"),
    ])?;
    let results = df.collect().await?;
    pretty::print_batches(&results)?;
    Ok(())
}

