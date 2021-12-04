
import org.slf4j.LoggerFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, datediff}

val PROJECT_NAME: String = "your_project_name"

class FlowMetricsApp {

  private val log: org.slf4j.Logger = LoggerFactory.getLogger(Predef.classOf[FlowMetricsApp])

  def start(): Unit = {
    //create a session
    val spark: SparkSession = SparkSession.builder()
      .appName("Flow Metrics")
      .master("local[*]")
      .getOrCreate()

    val valueStreamDF: _root_.org.apache.spark.sql.DataFrame = spark.read.format("bigquery")
      .option("credentialsFile", "src/main/resources/key/{PROJECT_NAME}.json")
      .option("table", "flow_models:items")
      .load()
      .cache()

    valueStreamDF.createOrReplaceTempView("flow_item_summary")

    log.debug("Num records in valueStream : {}", valueStreamDF.count())

    // using spark sql.
    val flowTimeSQL: String = "SELECT project, flow_item, AVG(DATE_DIFF(closed_at, started_at)) as avg_flow_time_days ".+("FROM flow_item_summary GROUP BY project, flow_item")

    val flowTimeDF: _root_.org.apache.spark.sql.DataFrame = spark.sql(flowTimeSQL)
    flowTimeDF.show(20)

    //Using dataframe API
    val avgFlowTimeDF: _root_.org.apache.spark.sql.DataFrame = valueStreamDF.withColumn("flow_time_days"
      , datediff(col("closed_at"), col("started_at")))
      .groupBy(col("project"), col("flow_item")).agg(
      avg("flow_time_days")
    )

    avgFlowTimeDF.show(20)

  }


  /*

      Schema -
      flow_item, flow_item_type, created_at, updated_at, status, assignee, project

      Table : - flow_item_summary
      flow_item, flow_item_type, started_at, closed_at, assignee, project

     */

}


object FlowMetrics {


  /**
    * Application entry point
    *
    * @param args command line arguments
    */
  def main(args: Array[String]): Unit = {
    val app: FlowMetricsApp = new FlowMetricsApp
    app.start()

  }


}
