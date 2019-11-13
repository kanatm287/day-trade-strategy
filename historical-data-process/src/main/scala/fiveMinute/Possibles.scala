package fiveMinute

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions._
import java.sql.Timestamp


class Possibles(tickerParams: TickerParams) {

  private val spark = tickerParams.spark

  private val ticker = tickerParams.ticker

  private def _generate(strategy: String,
                        spark: org.apache.spark.sql.SparkSession,
                        benchmarkColumnName: String,
                        dataset: Dataset[Row]): Unit = {

    import spark.implicits._

    val volumeDeltaPossibleList = dataset
      .groupBy("volume_delta").count().where("count >= 2").map(r => r.getAs[Double](0)).collect()

    val nextDataset = dataset
      .filter(col("volume_delta").isin(volumeDeltaPossibleList:_*))

    val openDeltaPossibleList = nextDataset
      .groupBy("open_delta").count().where("count >= 2").map(r => r.getAs[Double](0)).collect()

    val lastDataset = nextDataset
      .filter(col("open_delta").isin(openDeltaPossibleList:_*))

    val benchmarkDeltaPossibleList = lastDataset
      .groupBy(benchmarkColumnName).count().where("count >= 2").map(r => r.getAs[Double](0)).collect()

    val sessionPeriod = dataset.select("session_period").distinct.first().getInt(0)

    tickerParams.setPreProcessParams(
      tickerParams.preProcessParams()
        .withColumn("volume_delta_possibles",
          when(col("session_period") === sessionPeriod && col("strategy") === strategy,
            volumeDeltaPossibleList)
            .otherwise(col("volume_delta_possibles")))
        .withColumn("open_delta_possibles",
          when(col("session_period") === sessionPeriod && col("strategy") === strategy,
            openDeltaPossibleList)
            .otherwise(col("open_delta_possibles")))
        .withColumn("benchmark_delta_possibles",
          when(col("session_period") === sessionPeriod && col("strategy") === strategy,
            benchmarkDeltaPossibleList)
            .otherwise(col("benchmark_delta_possibles")))
        .withColumn("non_outliers_possibles_dates",
          when(col("session_period") === sessionPeriod && col("strategy") === strategy,
            lastDataset
              .filter(col(benchmarkColumnName).isin(benchmarkDeltaPossibleList:_*))
              .select("date").map(r => r.getAs[Timestamp](0)).collect())
            .otherwise(col("non_outliers_possibles_dates"))))
  }

  def generate(): Unit = {

    (for (i <- 1 until 7 by 1) yield tickerParams.trendDataset().filter(col("session_period") === i))
      .foreach(_generate("trend",
                         tickerParams.spark,
                         if (tickerParams.trend.spy) "spy_open_delta" else "benchmark_open_delta",
                         _))

    (for (i <- 1 until 7 by 1) yield tickerParams.correctionDataset().filter(col("session_period") === i))
      .foreach(_generate("correction",
               tickerParams.spark,
               if (tickerParams.correction.spy) "spy_open_delta" else "benchmark_open_delta",
               _))
  }

  private def _filter(dataset: Dataset[Row], benchmarkColumnName: String): Dataset[Row] = {

    dataset
      .join(tickerParams.preProcessParams()
        .drop("date", "ticker"),
        Seq("session_period", "strategy"),
        "right")
      .filter(array_contains(col("non_outliers_possibles_dates"), col("date")))

  }

  def filter(): Unit = {

    tickerParams.setTrendDataset(
      _filter(tickerParams.trendDataset(),
        if (tickerParams.trend.spy) "spy_open_delta" else "benchmark_open_delta"))

    tickerParams.setCorrectionDataset(
      _filter(tickerParams.correctionDataset(),
        if (tickerParams.correction.spy) "spy_open_delta" else "benchmark_open_delta"))
  }
}
