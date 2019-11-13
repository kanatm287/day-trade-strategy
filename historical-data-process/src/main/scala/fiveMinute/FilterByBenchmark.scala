package fiveMinute

import org.apache.spark.sql.functions._
import java.sql.Timestamp


trait FilterByBenchmark {

  def filterByBenchmark(tickerParams: TickerParams): Unit = {

    val spark =tickerParams.spark

    import spark.implicits._

    if (!tickerParams.trend.benchmark_negative.isEmpty & !tickerParams.trend.benchmark_positive.isEmpty) {
      tickerParams.setTrendDataset(
        if (tickerParams.action == "buy") {
          tickerParams.dataset()
            .where("strategy = 'trend'")
            .filter(col(if (tickerParams.trend.spy) "spy_difference" else "benchmark_difference")
              .between(tickerParams.trend.benchmark_negative.get, tickerParams.trend.benchmark_positive.get))
            .withColumnRenamed(
              if (tickerParams.correction.spy) "spy_difference" else "benchmark_difference", "difference")
        } else {
          tickerParams.dataset()
            .where("strategy = 'trend'")
            .filter(col(if (tickerParams.trend.spy) "spy_difference" else "benchmark_difference")
              .between(tickerParams.trend.benchmark_positive.get, tickerParams.trend.benchmark_negative.get))
            .withColumnRenamed(
              if (tickerParams.correction.spy) "spy_difference" else "benchmark_difference", "difference")
        })
    } else {
      tickerParams.setTrendDataset(tickerParams.dataset().where("strategy = 'trend'"))
    }

    if (!tickerParams.correction.benchmark_negative.isEmpty & !tickerParams.correction.benchmark_positive.isEmpty) {
      tickerParams.setCorrectionDataset(
        if (tickerParams.action == "buy") {
          tickerParams.dataset()
            .where("strategy = 'correction'")
            .filter(!col(if (tickerParams.correction.spy) "spy_difference" else "benchmark_difference")
              .between(tickerParams.correction.benchmark_negative.get, tickerParams.correction.benchmark_positive.get))
            .withColumnRenamed(
              if (tickerParams.correction.spy) "spy_difference" else "benchmark_difference", "difference")
        } else {
          tickerParams.dataset()
            .where("strategy = 'correction'")
            .filter(!col(if (tickerParams.correction.spy) "spy_difference" else "benchmark_difference")
              .between(tickerParams.correction.benchmark_positive.get, tickerParams.correction.benchmark_negative.get))
            .withColumnRenamed(
              if (tickerParams.correction.spy) "spy_difference" else "benchmark_difference", "difference")
        })
    } else {
      tickerParams.setCorrectionDataset(tickerParams.dataset().where("strategy = 'correction'"))
    }

    List(1, 2, 3, 4, 5, 6).foreach{sessionPeriod =>
      tickerParams.setPreProcessParams(
        tickerParams.preProcessParams()
          .withColumn("benchmark_possibles_dates",
            when(col("session_period") === sessionPeriod && col("strategy") === "trend",
              tickerParams.trendDataset()
                .filter(col("session_period") === sessionPeriod)
                .select("date").map(r => r.getAs[Timestamp](0)).collect())
              .otherwise(col("benchmark_possibles_dates")))
          .withColumn("benchmark_possibles_dates",
            when(col("session_period") === sessionPeriod && col("strategy") === "correction",
              tickerParams.correctionDataset()
                .filter(col("session_period") === sessionPeriod)
                .select("date").map(r => r.getAs[Timestamp](0)).collect())
              .otherwise(col("benchmark_possibles_dates"))))
    }
  }
}
