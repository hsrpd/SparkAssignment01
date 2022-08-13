package copyOfDemo.Cohort

import copyOfDemo.Cohort.CohortFunctionality._
import org.apache.spark.sql._

class Cohort {
  def calculateCohort(df: DataFrame, weekDate: String, weekRange: Int):DataFrame ={

    val dateToday = getLocalDate(weekDate)
    val mp = getWeekMap()

    val weekDates = getWeekDates(weekRange, dateToday, mp)
    val dateStartWeek = weekDates(0)
    val dateEndWeek = weekDates(1)

    val riderWeeklyMatrix = getRiderWeeklyMatrix(df, dateStartWeek, dateEndWeek)
    val size = riderWeeklyMatrix.length
    printMatrix(riderWeeklyMatrix, size)

    df
  }
}
