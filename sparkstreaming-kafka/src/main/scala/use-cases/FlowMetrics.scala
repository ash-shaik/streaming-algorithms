
import org.apache.spark.sql.SparkSession

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

     valueStreamDF.createOrReplaceTempView("flow_events")
  }





}
