package copyOfDemo
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties
import scala.io.Source
import scala.language.implicitConversions

object copyAddNumbers extends Serializable {
  def main(args:Array[String]): Unit ={

    if(args.length == 0){
      System.exit(1)
    }

    val spark = SparkSession.builder()
      .config(getConf)
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(1))
    df.createOrReplaceTempView("assignmentDataView")

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//    val df_hourly = spark.sql("select number as user, extract(hour from ts) as hh, count(*) as task " +
//      "from assignmentDataView " +
//      "group by number, extract(hour from ts) "
//    )
//    val df_hourly_pivot = df_hourly.groupBy("user").pivot("hh").sum("task")
//    val df_hourly_pivot_fill0_order = df_hourly_pivot.na.fill(0).orderBy("user")
//    df_hourly_pivot_fill0_order.show()

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//    val df_daily = spark.sql("select number as user, extract(day from ts) as date, count(*) as task " +
//      "from assignmentDataView " +
//      "group by number, extract(day from ts) "
//    )
//    val df_daily_pivot = df_daily.groupBy("user").pivot("date").sum("task")
//    val df_daily_pivot_fill0_order = df_daily_pivot.na.fill(0).orderBy("user")
//    df_daily_pivot_fill0_order.show()

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//    val df_weekly = spark.sql("select number as user, date_format(ts, 'E') as date,  count(*) as task " +
//      "from assignmentDataView " +
//      "group by number, date_format(ts, 'E') "
//    )
//
//    val df_weekly_pivot = df_weekly.groupBy("user").pivot("date").sum("task")
//    val df_weekly_pivot_fill0_order = df_weekly_pivot.na.fill(0).orderBy("user")
//    df_weekly_pivot_fill0_order.show()

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//    val df_monthly = spark.sql("select number as user, date_format(ts, 'MMM') as date,  count(*) as task " +
//      "from assignmentDataView " +
//      "group by number, date_format(ts, 'MMM') "
//    )
//
//    val df_monthly_pivot = df_monthly.groupBy("user").pivot("date").sum("task")
//    val df_monthly_pivot_fill0_order = df_monthly_pivot.na.fill(0).orderBy("user")
//    df_monthly_pivot_fill0_order.show()

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    spark.stop()
  }

  def getConf: SparkConf = {
    val sc = new SparkConf()

    val props = new Properties
    props.load(Source.fromFile("spark.conf").bufferedReader())

    import scala.collection.JavaConverters.dictionaryAsScalaMapConverter
    props.asScala.foreach(kv => sc.set(kv._1.toString, kv._2.toString))
    sc
  }

  def getDF(spark: SparkSession, data:String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(data)
  }

}
