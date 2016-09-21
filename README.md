# Spark Catalyst Deep Dive

#Introduction



![Catalyst Workflow](./docs/images/catalyst-flow.png "Catalyst Workflow")

As the workflow diagram shows, there are two main methods to create a QueryPlan:
1. From a DataFrame / Dataset
2. By parsing a SQL text

```scala
type DataFrame = Dataset[Row]
```

A Dataset is a strongly typed collection of domain-specific objects that can be transformed
in parallel using functional or relational operations. Each Dataset also has an untyped view
called a [[DataFrame]], which is a Dataset of [[Row]].

Datasets are "lazy", i.e. computations are only triggered when an action is invoked. Internally,
a Dataset represents a logical plan that describes the computation required to produce the data.
When an action is invoked, Spark's query optimizer optimizes the logical plan and generates a
physical plan for efficient execution in a parallel and distributed manner. To explore the
logical plan as well as optimized physical plan, use the `explain` function.

```scala
class Dataset[T] private[sql](
    @transient val sparkSession: SparkSession,
    @DeveloperApi @transient val queryExecution: QueryExecution,
    encoder: Encoder[T])
  extends Serializable {
  
  ...
    def this(sparkSession: SparkSession, logicalPlan: LogicalPlan, encoder: Encoder[T]) = {
      this(sparkSession, sparkSession.sessionState.executePlan(logicalPlan), encoder)
    }
  ...
```

We notice that the default constructor is private, but there is one public constructor that can be used.
If we have a look on how the Datasets are created we notice a few simple use cases is `SparkSession`:

```scala
...
  def emptyDataset[T: Encoder]: Dataset[T] = {
    val encoder = implicitly[Encoder[T]]
    new Dataset(self, LocalRelation(encoder.schema.toAttributes), encoder)
  }
...
  def createDataset[T : Encoder](data: Seq[T]): Dataset[T] = {
    val enc = encoderFor[T]
    val attributes = enc.schema.toAttributes
    val encoded = data.map(d => enc.toRow(d).copy())
    val plan = new LocalRelation(attributes, encoded)
    Dataset[T](self, plan)
  }
...
  def createDataset[T : Encoder](data: RDD[T]): Dataset[T] = {
    val enc = encoderFor[T]
    val attributes = enc.schema.toAttributes
    val encoded = data.map(d => enc.toRow(d))
    val plan = LogicalRDD(attributes, encoded)(self)
    Dataset[T](self, plan)
  }
```




```scala
class QueryExecution(val sparkSession: SparkSession, val logical: LogicalPlan) {
  ...
  lazy val analyzed: LogicalPlan = {
    SparkSession.setActiveSession(sparkSession)
    sparkSession.sessionState.analyzer.execute(logical)
  }
  ...
  lazy val optimizedPlan: LogicalPlan = sparkSession.sessionState.optimizer.execute(withCachedData)
  ...
  
  lazy val sparkPlan: SparkPlan = {
    SparkSession.setActiveSession(sparkSession)
    planner.plan(ReturnAnswer(optimizedPlan)).next()
  }
  ...
  
  // executedPlan should not be used to initialize any SparkPlan. It should be
  // only used for execution.
  lazy val executedPlan: SparkPlan = prepareForExecution(sparkPlan)

}
```





## References:
 
[Spark SQL: Relational Data Processing in Spark](http://people.csail.mit.edu/matei/papers/2015/sigmod_spark_sql.pdf)

[Deep Dive into Spark SQL’s Catalyst Optimizer](https://databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html)
 
[anatomy-of-spark-catalyst](https://github.com/phatak-dev/anatomy-of-spark-catalyst)
