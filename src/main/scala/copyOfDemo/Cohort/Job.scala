package copyOfDemo.Cohort

import copyOfDemo.{Reader, Writer}

class Job(reader: Reader, writer: Writer) {
  def run(sourcePath: String, destinationPath: String, weekDate: String, weekRange: Int): Any = {

    val inputDF = reader.read(sourcePath)
    val cohort = new Cohort()

    val outputDf = cohort.calculateCohort(inputDF, weekDate, weekRange)
//    writer.write(outputDf, destinationPath)
  }
}
