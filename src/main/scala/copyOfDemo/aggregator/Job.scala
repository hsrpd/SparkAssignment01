package copyOfDemo.aggregator

import copyOfDemo.{Reader, Writer}

class Job(reader: Reader, writer: Writer) {
  def run(sourcePath: String, destinationPath: Array[String]): Any = {

    val inputDF = reader.read(sourcePath)
    val aggregator = new Aggregator()

    val n = destinationPath.length
    for(i <- 0 until n){
      val outputDF = aggregator.aggregate(inputDF, i)
      writer.write(outputDF, destinationPath(i))

      println(s"Job $i completed!")
    }

  }
}
