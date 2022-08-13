package copyOfDemo.aggregator

import copyOfDemo.aggregator.Driver.spark
import org.apache.spark.sql.DataFrame

object AggregatorFunctionality {

  def hourlyAggregation(view: String): DataFrame ={
    val df = spark.sql("select number as user, extract(hour from ts) as ts, count(*) as task " +
      "from " + view +
      " group by number, extract(hour from ts) "
    )
    df
  }

  def dailyAggregation(view: String): DataFrame ={
    val df = spark.sql("select number as user, extract(day from ts) as ts, count(*) as task " +
      "from " + view +
      " group by number, extract(day from ts) "
    )
    df
  }

  def weeklyAggregation(view: String): DataFrame = {
    val df = spark.sql("select number as user, date_format(ts, 'E') as ts,  count(*) as task " +
      "from " + view +
      " group by number, date_format(ts, 'E') "
    )
    df
  }

  def monthlyAggregation(view: String): DataFrame = {
    val df = spark.sql("select number as user, date_format(ts, 'MMM') as ts,  count(*) as task " +
      "from " + view +
      " group by number, date_format(ts, 'MMM') "
    )
    df
  }

}
