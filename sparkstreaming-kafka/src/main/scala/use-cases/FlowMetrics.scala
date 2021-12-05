
import org.slf4j.LoggerFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, datediff, current_date, months_between, lit}

val PROJECT_NAME: String = "your_project_name"

class FlowMetricsApp {
  /**
    * App reads dataset from bigQuery and builds out the following Flow Metrics :
    * 1. Flow Time
    * 2. Flow Distribution
    * 3. Flow Velocity
    * 4. Flow Load
    * 5. Flow Efficiency
    */
  private val log: org.slf4j.Logger = LoggerFactory.getLogger(Predef.classOf[FlowMetricsApp])

  def start(): Unit = {
    //Create a session
    val spark: SparkSession = SparkSession.builder()
      .appName("Flow Metrics")
      .master("local[*]")
      .getOrCreate()

    // Read Dataset from BigQuery
    val valueStreamSummaryDF: org.apache.spark.sql.DataFrame = spark.read.format("bigquery")
      .option("credentialsFile", "src/main/resources/key/{PROJECT_NAME}.json")
      .option("table", "flow_models:item_summary")
      .load()
      .cache()

    valueStreamSummaryDF.createOrReplaceTempView("flow_item_summary")
    /*
      Schema -
      Table : - flow_item_summary
      -- flow_item : integer (nullable = false)
      -- flow_item_type : string (nullable = false)
      -- status : string (nullable = false)
      -- started_at : datetime (nullable = false)
      -- closed_at : datetime (nullable = true)
      -- days_in_lead : integer (nullable = true)
      -- assignee : string (nullable = true)
      -- project : string (nullable = false)
     */

    log.debug("Num records in valueStream : {}", valueStreamSummaryDF.count())

    // Flow Time: using spark sql.
    val flowTimeSQL: String = {
      "SELECT project, flow_item, AVG(DATE_DIFF(closed_at, started_at)) as avg_flow_time_days ".+("FROM flow_item_summary GROUP BY project, flow_item")
    }

    val flowTimeDF: org.apache.spark.sql.DataFrame = spark.sql(flowTimeSQL)
    flowTimeDF.show(20)

    // Using Data frame API
    // Flow Time : Average time to compete a flow in days.
    val avgFlowTimeDF: org.apache.spark.sql.DataFrame = valueStreamSummaryDF.withColumn("flow_time_days"
      , datediff(col("closed_at"), col("started_at")))
      .groupBy(col("project"), col("flow_item")).agg(
      avg("flow_time_days").as("flow_time_days")
    )

    avgFlowTimeDF.show(20)

    //Unique Projects
    val uniqueProjectsDF: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] =
      valueStreamSummaryDF.select("project").distinct()
    uniqueProjectsDF.show()


    // Count is an action - and its returns a value immediately.
    val total: Long = valueStreamSummaryDF.count()
    // Flow Distribution : Count by flow item type -  % of Total .
    valueStreamSummaryDF.groupBy(col("flow_item")).count()
      .withColumnRenamed("count", "count_per_item_type")
      .withColumn("percentage_of_total", col("count_per_item_type")./(total).*(100))
      .sort(col("percentage_of_total").desc_nulls_last)


    // Flow Velocity : Number of flow items completed in last 3 months.
    /* val completedInLast3MonsDF = valueStreamDF.filter(
      col("closed_at").between("2021-09-01", "2021-12-01")
        .&&(col("status").equalTo("completed"))
      )*/

    val completedInLast3MonsDF: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] =
      valueStreamSummaryDF.withColumn("current_date", current_date())
      .withColumn("monthsDiff", months_between(col("closed_at"), col("current_date")))
      .filter(
      col("monthsDiff").<=(lit(3))
    )


    val flowVelocityDF: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] =
      completedInLast3MonsDF.groupBy(col("flow_item")).count()
      .withColumnRenamed("count", "countPerItemType")
      .sort("flow_item")
    flowVelocityDF.show()

    //Flow Load : Number of items currently in progress
    val currentlyInProgressDF: org.apache.spark.sql.DataFrame = valueStreamSummaryDF.
      filter(col("status").equalTo("in_progress")
    ).groupBy(col("flow_item")).count()

    currentlyInProgressDF.show()

    //Flow Efficiency : Ratio of Avg Lead Time to Avg Flow Time in days

    val avgLeadTimeDF: org.apache.spark.sql.DataFrame =
      valueStreamSummaryDF.groupBy(col("flow_item"))
      .agg(avg("days_in_lead").as("avg_lead_time"))


    avgLeadTimeDF.join(avgFlowTimeDF
      , avgLeadTimeDF.apply("flow_item").===(avgFlowTimeDF.apply("flow_item")), "inner")
      .withColumn("flow_efficiency"
        , col("days_in_lead")./(col("flow_time_days")).*(100)).show()

    spark.stop()

  }
}


object FlowMetrics {
  /**
    * Application entry point
    *
    * @param args startup arguments
    */
  def main(args: Array[String]): Unit = {
    val app: FlowMetricsApp = new FlowMetricsApp
    app.start()

  }


}
