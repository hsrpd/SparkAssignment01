package copyOfDemo.aggregator

import copyOfDemo.{Reader, SparkUtils, Writer}

object Driver extends SparkUtils{
  val reader = new Reader(spark)
  val writer = new Writer(spark)

  def main(args: Array[String]): Unit = {

    println("Reading Source Path...")
    val sourcePath = args(0)

    println("Reading Destination Path...")
    val n = args.length
    val destinationPath = new Array[String](n-1)

    for(i <- 1 until n){
      destinationPath(i-1) = args(i)
    }

    println("Running 4 Jobs...")
    val job = new Job(reader, writer)
    job.run(sourcePath, destinationPath)

    spark.stop()
  }
}
