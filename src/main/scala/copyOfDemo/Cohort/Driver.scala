package copyOfDemo.Cohort

import copyOfDemo.{Reader, SparkUtils, Writer}

object Driver extends SparkUtils{
  val reader = new Reader(spark)
  val writer = new Writer(spark)

  def main(args: Array[String]): Unit = {
    val sourcePath = args(0)
    val destinationPath = args(1)

    print("Enter Week Date : ")
    val weekDate = scala.io.StdIn.readLine()

    print("Enter Week Range : ")
    val weekRange = scala.io.StdIn.readInt()

    println("Job is running...")
    val job = new Job(reader, writer)
    job.run(sourcePath, destinationPath, weekDate, weekRange)
    println("Job completed...")

    spark.stop()
  }
}
