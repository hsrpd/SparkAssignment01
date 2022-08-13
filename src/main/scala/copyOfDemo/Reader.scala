package copyOfDemo

import org.apache.spark.sql.{DataFrame, SparkSession}

class Reader(spark: SparkSession) {
  def read(sourcePath: String): DataFrame ={
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(sourcePath)
  }
}
