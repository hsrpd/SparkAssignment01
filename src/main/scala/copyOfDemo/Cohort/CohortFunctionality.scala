package copyOfDemo.Cohort

import copyOfDemo.Cohort.Driver.spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, to_date}

import java.time._
import java.time.format._

object CohortFunctionality {

  def getLocalDate(weekDate: String): LocalDate ={
    val format = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val dateToday = LocalDate.parse(weekDate, format)
    dateToday
  }

  def getWeekMap(): Map[String, Int] ={
    val mp = Map(
      "MONDAY" -> 0,
      "TUESDAY" -> 1,
      "WEDNESDAY" -> 2,
      "THURSDAY" -> 3,
      "FRIDAY" -> 4,
      "SATURDAY" -> 5,
      "SUNDAY" -> 6
    )
    mp
  }

  def getWeekDates(weekRange: Int, dateToday: LocalDate, mp: Map[String, Int]): List[List[LocalDate]] ={
    var start = dateToday.minusDays(mp(dateToday.getDayOfWeek.toString))
    var end = dateToday.plusDays(6 - mp(dateToday.getDayOfWeek.toString))

    var dateStartWeek: List[LocalDate] = Nil
    var dateEndWeek: List[LocalDate] = Nil

    for(i <- 0 until weekRange){
      dateStartWeek = dateStartWeek :+ start
      dateEndWeek = dateEndWeek :+ end

      start = start.plusDays(7)
      end = end.plusDays(7)
    }
    List(dateStartWeek, dateEndWeek)
  }

  def getRiderWeeklyMatrix(df: DataFrame, dateStartWeek: List[LocalDate], dateEndWeek: List[LocalDate]): Array[Array[Integer]] ={
    var df2 = spark.emptyDataFrame
    df2 = df.withColumn("temp", to_date(col("ts")))
    df2.createOrReplaceTempView("addedDateView")

    val df_firstRide = spark.sql("select number, min(temp) as temp from addedDateView group by number")
    df_firstRide.show()

    val size = dateStartWeek.length
    var arr = Array.ofDim[Integer](size, size)

    for(i:Int <- 0 until size){
      var df_original = spark.emptyDataFrame
      var df_new_users = spark.emptyDataFrame
      var df_previous_week_users = spark.emptyDataFrame

      for(j:Int <- i until size){
        df_original = df2.filter(df2("temp") >= (dateStartWeek(j).toString) and df2("temp") <= (dateEndWeek(j).toString)).groupBy("number").count().select("number")

        if(j == i){
          df_new_users = df_firstRide.filter( df_firstRide("temp") >= (dateStartWeek(j).toString) and df_firstRide("temp") <= (dateEndWeek(j).toString) ).select("number")
          df_original = df_original.intersect(df_new_users)
        }
        if(j>i){
          df_original = df_original.intersect(df_previous_week_users)
        }

        arr(i)(j) = df_original.count().toInt
        df_previous_week_users = df_original.select("number")
      }
    }
    arr
  }

  def printMatrix(arr: Array[Array[Integer]], size: Int) = {
    for (i <- 0 until size) {
      for (j <- 0 until size) {
        print(" " + arr(i)(j))
      }
      println(" ")
    }
  }

}
