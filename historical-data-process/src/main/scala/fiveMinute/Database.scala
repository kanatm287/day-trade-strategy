package fiveMinute

import java.sql.{Connection, DriverManager, Timestamp}
import org.apache.spark.sql.{Dataset, Row}


trait Database {

  private def prop(s:String) = {  System.getProperty(s) }

  def benchmark(ticker: String, spark: org.apache.spark.sql.SparkSession): String = {

    spark
      .read
      .format("jdbc")
      .option("url", "jdbc:postgresql:" + prop("ibdb.name"))
      .option("user", prop("ibdb.user"))
      .option("password", prop("ibdb.password"))
      .option("query",
        "select benchmark " +
          "from benchmark_utils " +
          "where industry = (select industry " +
          "from trade_params " +
          "where ticker = '" + ticker +  "')")
      .load()
      .first()
      .get(0)
      .asInstanceOf[String]
  }

  def testParams(ticker: String, spark: org.apache.spark.sql.SparkSession): Dataset[Row] = {

    spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql:" + prop("ibdb.name"))
      .option("user", prop("ibdb.user"))
      .option("password", prop("ibdb.password"))
      .option("query",
        "select test.strategy, test.technical_params, test.action, trade.volume_max, trade.volume_min " +
          "from test_params test inner join trade_params trade " +
          "on test.symbol = trade.ticker "+
          "where test.time_frame = 'minute' and test.symbol = '" + ticker +  "'")
      .load()
  }

  def initialData(ticker: String, spark: org.apache.spark.sql.SparkSession) = {

    spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql:" + prop("ibdb.name"))
      .option("user", prop("ibdb.user"))
      .option("password", prop("ibdb.password"))
      .option("query",
        "select date, open, high, low, close, volume " +
          "from historical_five_minute_data " +
          "where ticker = '" + ticker + "' " +
          "order by date asc")
      .load()
      .asInstanceOf[Dataset[Row]]
  }

  def preparedData(ticker: String, spark: org.apache.spark.sql.SparkSession) = {

    spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql:" + prop("ibdb.name"))
      .option("user", prop("ibdb.user"))
      .option("password", prop("ibdb.password"))
      .option("query",
        "select hist.date, hist.ticker, hist.close, test.strategy, test.day_max, test.day_min, " +
          "test.volume_delta, test.open_delta, test.benchmark_open_delta, test.spy_open_delta, " +
          "test.session_period, test.prev_day_close, test.spy_difference, test.benchmark_difference " +
          "from historical_five_minute_data hist inner join test_five_minute_data test " +
          "on hist.date = test.date and hist.ticker = '" + ticker + "' and test.ticker = '" + ticker + "' " +
          "order by date asc")
      .load()
      .asInstanceOf[Dataset[Row]]
  }

  def preClassifyData(ticker: String, spark: org.apache.spark.sql.SparkSession) = {

    spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql:" + prop("ibdb.name"))
      .option("user", prop("ibdb.user"))
      .option("password", prop("ibdb.password"))
      .option("query",
        "select test.date, test.ticker, test.strategy, test.day_max, test.day_min, " +
          "test.volume_delta, test.open_delta, test.benchmark_open_delta, test.spy_open_delta, " +
          "test.session_period, test.prev_day_close, test.spy_difference, test.benchmark_difference, " +
          "test.high_delta, test.low_delta, test.high_stop, test.low_stop, " +
          "params.high_delta_exceptions_dates, params.high_stop_exceptions_dates, " +
          "params.low_delta_exceptions_dates, params.low_stop_exceptions_dates " +
          "from test_five_minute_data test inner join ml_pre_process_params params " +
          "on test.strategy = params.strategy and test.session_period = params.session_period " +
          "and test.ticker = '" + ticker + "' and params.ticker = '" + ticker + "' " +
          "where test.date in (select unnest(params.non_outliers_possibles_dates)) " +
          "and test.date in (select unnest(params.benchmark_possibles_dates)) " +
          "order by date asc")
      .load()
      .asInstanceOf[Dataset[Row]]
  }

  def prePredictData(ticker: String, spark: org.apache.spark.sql.SparkSession) = {

    spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql:" + prop("ibdb.name"))
      .option("user", prop("ibdb.user"))
      .option("password", prop("ibdb.password"))
      .option("query",
        "select test.date, test.ticker, test.strategy, test.day_max, test.day_min, " +
          "test.volume_delta, test.open_delta, test.benchmark_open_delta, test.spy_open_delta, " +
          "test.session_period, test.prev_day_close, test.spy_difference, test.benchmark_difference, " +
          "params.high_delta_exceptions_dates, params.high_stop_exceptions_dates, " +
          "params.low_delta_exceptions_dates, params.low_stop_exceptions_dates, " +
          "params.non_outliers_possibles_dates, params.benchmark_possibles_dates " +
          "from test_five_minute_data test inner join ml_pre_process_params params " +
          "on test.strategy = params.strategy and test.session_period = params.session_period " +
          "and test.ticker = '" + ticker + "' and params.ticker = '" + ticker + "' " +
          "order by date asc"
//          "limit 78 * 60"
      )
      .load()
      .asInstanceOf[Dataset[Row]]
  }

  def preProcessParams(ticker: String, spark: org.apache.spark.sql.SparkSession): Dataset[Row] = {

    spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql:" + prop("ibdb.name"))
      .option("user", prop("ibdb.user"))
      .option("password", prop("ibdb.password"))
      .option("query",
        "select date, ticker, session_period, strategy, " +
          "volume_delta_possibles, open_delta_possibles, benchmark_delta_possibles, " +
          "high_delta_exceptions, high_stop_exceptions, low_delta_exceptions, low_stop_exceptions " +
          "high_delta_exceptions_dates, high_stop_exceptions_dates, " +
          "low_delta_exceptions_dates, low_stop_exceptions_dates " +
          "from ml_pre_process_params " +
          "where ticker = '" + ticker + "' " +
          "order by date asc")
      .load()
      .asInstanceOf[Dataset[Row]]
  }

  def classifiers(ticker: String, spark: org.apache.spark.sql.SparkSession): Dataset[Row] = {

    spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql:" + prop("ibdb.name"))
      .option("user", prop("ibdb.user"))
      .option("password", prop("ibdb.password"))
      .option("query",
        "select date, ticker, session_period, strategy, " +
          "high_delta_classifier, high_stop_classifier, low_delta_classifier, low_stop_classifier " +
          "from ml_classifiers " +
          "where ticker = '" + ticker + "'")
      .load()
      .asInstanceOf[Dataset[Row]]
  }

  private def deleteFromTable(ticker: String, tableName: String): Unit = {

    val driver = "org.postgresql.Driver"
    val url = "jdbc:postgresql://localhost/" + prop("ibdb.name")
    val username = prop("ibdb.user")
    val password = prop("ibdb.password")

    var connection:Connection = null

    try {

      Class.forName(driver)

      connection = DriverManager.getConnection(url, username, password)

      val statement = connection.createStatement()

      if (statement.executeQuery(
        "select distinct(ticker) from " + tableName + " where ticker = '" + ticker + "'").next()) {
        statement.executeUpdate("delete from " + tableName + " where ticker = '" + ticker + "'")
      } else {
        None
      }
    } catch {
      case e => e.printStackTrace
    }
    connection.close()
  }

  def updateDeltas(dataset: Dataset[Row]): Unit = {

    val driver = "org.postgresql.Driver"
    val url = "jdbc:postgresql://localhost/" + prop("ibdb.name")
    val username = prop("ibdb.user")
    val password = prop("ibdb.password")

    var connection:Connection = null

    try {

      Class.forName(driver)

      connection = DriverManager.getConnection(url, username, password)

      val statement = connection.createStatement()

      dataset.collect()
        .foreach{row =>

          statement.executeUpdate(
            "update public.test_five_minute_data" +
              " set high_delta = " + row.getAs[Boolean]("high_delta") + ", " +
              " low_delta = " + row.getAs[Boolean]("low_delta") + ", " +
              " high_stop = " + row.getAs[Boolean]("high_stop") + ", " +
              " low_stop = " + row.getAs[Boolean]("low_stop") +
              " where ticker = '" + row.getAs[String]("ticker") + "'" +
              " and date = '" + row.getAs[Timestamp]("date") + "'")
      }
    } catch {
      case e => e.printStackTrace
    }
    connection.close()
  }

  def writeDatasetToDatabase(tableName: String,
                             spark: org.apache.spark.sql.SparkSession,
                             dataset: Dataset[Row]): Unit = {

    deleteFromTable(dataset.select("ticker").distinct.first().getString(0), tableName)

    dataset
      .write
      .mode("append")
      .format("jdbc")
      .option("url", "jdbc:postgresql:" + prop("ibdb.name"))
      .option("dbtable", tableName)
      .option("user", prop("ibdb.user"))
      .option("password", prop("ibdb.password"))
      .save()
  }
}
