package copyOfDemo
import org.apache.spark.sql.SparkSession

trait SparkUtils {
  val spark: SparkSession = SparkSession.builder()
    .config("spark.app.name", "Spark Assignment 01")
    .config("spark.master", "local[3]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.allowMultipleContexts", true)
    .getOrCreate()
}
