
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, datediff, col}

val PROJECT_NAME = "yourprojectname"

object FlowMetrics{

  /**
    * Application entry point
    * @param args
    */
   def main(args: Array[String]): Unit = {

    //create a session
    val spark = SparkSession.builder()
      .appName("Flow Metrics")
      .master("local[*]")
      .getOrCreate()

    val valueStreamDF = spark.read.format("bigquery")
      .option("credentialsFile", "src/main/resources/key/{PROJECT_NAME}.json")
      .option("table", "flow_models:items")
      .load()
      .cache()

     valueStreamDF.createOrReplaceTempView("flow_item_summary")

     // using spark sql.
     val flowTimeSQL = "SELECT project, flow_item, AVG(DATE_DIFF(closed_at, started_at)) as avg_flow_time_days " +
       "FROM flow_item_summary GROUP BY project, flow_item"

     val flowTimeDF = spark.sql(flowTimeSQL)

     //Using dataframe API
     val avgFlowTimeDF = valueStreamDF.withColumn("flow_time_days"
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
