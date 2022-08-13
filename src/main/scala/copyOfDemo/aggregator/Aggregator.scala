package copyOfDemo.aggregator

import copyOfDemo.aggregator.AggregatorFunctionality._
import copyOfDemo.aggregator.Driver.spark
import org.apache.spark.sql.DataFrame

class Aggregator {
  def aggregate(df: DataFrame, i: Int): DataFrame = {
    df.createOrReplaceTempView("assignmentDataView")

    var df_sql = spark.emptyDataFrame

    i match {
      case 0 => df_sql = hourlyAggregation("assignmentDataView")
      case 1 => df_sql = dailyAggregation("assignmentDataView")
      case 2 => df_sql = weeklyAggregation("assignmentDataView")
      case 3 => df_sql = monthlyAggregation("assignmentDataView")
    }
//    // Hourly Aggregation
//    if(i == 0) {
//      df_sql = spark.sql("select number as user, extract(hour from ts) as ts, count(*) as task " +
//        "from assignmentDataView " +
//        "group by number, extract(hour from ts) "
//      )
//    }
//
//    // Daily Aggregation
//    else if(i == 1) {
//      df_sql = spark.sql("select number as user, extract(day from ts) as ts, count(*) as task " +
//        "from assignmentDataView " +
//        "group by number, extract(day from ts) "
//      )
//    }
//
//    // Weekly Aggregation
//    else if(i == 2){
//      df_sql = spark.sql("select number as user, date_format(ts, 'E') as ts,  count(*) as task " +
//        "from assignmentDataView " +
//        "group by number, date_format(ts, 'E') "
//      )
//    }
//
//    // Monthly Aggregation
//    else if(i==3){
//      df_sql = spark.sql("select number as user, date_format(ts, 'MMM') as ts,  count(*) as task " +
//        "from assignmentDataView " +
//        "group by number, date_format(ts, 'MMM') "
//      )
//    }

    df_sql = df_sql.groupBy("user").pivot("ts").sum("task")
    val df_pivot_fill0_order = df_sql.na.fill(0).orderBy("user")

    df_pivot_fill0_order

  }
}
