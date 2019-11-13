package fiveMinute

import java.sql.Timestamp

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions._

import scala.collection.mutable._
import net.liftweb.json._
import net.liftweb.json.Serialization.write


class Exceptions(tickerParams: TickerParams) {

  private def _generate(strategy: String,
                        benchmarkColumnName: String,
                        deltas: List[String],
                        spark: org.apache.spark.sql.SparkSession,
                        dataset: Dataset[Row]): Unit = {

    import spark.implicits._

    implicit val format = Serialization.formats(NoTypeHints)

    Seq(1, 2, 3, 4, 5, 6)
      .foreach{ sessionPeriod =>

      deltas.foreach{ delta =>

        val singlesRemoved = dataset
          .filter(col("session_period") === sessionPeriod)
          .orderBy(col("open_delta"), col(benchmarkColumnName))
          .cube(col("open_delta"), col(benchmarkColumnName))
          .count()
          .orderBy(col("open_delta"), col(benchmarkColumnName))
          .filter(col("open_delta").isNotNull
            && col(benchmarkColumnName).isNotNull)

        val exceptionsFiltered = dataset
          .filter(col("session_period") === sessionPeriod)
          .join(singlesRemoved, Seq("open_delta", benchmarkColumnName), "left")
          .orderBy(col("open_delta"), col(benchmarkColumnName))
          .filter(col("count") > 1)
          .groupBy(col("open_delta"), col(benchmarkColumnName))
          .agg(countDistinct(delta).alias(delta + "_exceptions_count"))
          .filter(col(delta + "_exceptions_count") > 1)

        val exceptionsDates = dataset
          .filter(col("session_period") === sessionPeriod)
          .join(exceptionsFiltered, Seq("open_delta", benchmarkColumnName), "left" )
          .orderBy(col("open_delta"), col(benchmarkColumnName))
          .filter(col(delta + "_exceptions_count").isNotNull)
          .select("date").map(r => r.getAs[Timestamp](0)).collect()

        tickerParams.setPreProcessParams(
          tickerParams.preProcessParams()
            .withColumn(delta + "_exceptions",
              when(col("session_period") === sessionPeriod && col("strategy") === strategy,
                write(
                  exceptionsFiltered
                    .map(r => Array(r.getAs[Double](0), r.getAs[Double](1)))
                    .collect()
                  .toList))
                .otherwise(col(delta + "_exceptions")))
            .withColumn(delta + "_exceptions_dates",
              when(col("session_period") === sessionPeriod && col("strategy") === strategy,
                exceptionsDates)
                .otherwise(col(delta + "_exceptions_dates"))))
      }
    }
  }

  def generate(): Unit = {

    val possibles = new Possibles(tickerParams)

    possibles.filter()

    _generate(
      "trend",
      if (tickerParams.trend.spy) "spy_open_delta" else "benchmark_open_delta",
      if (tickerParams.action == "buy") List("high_delta", "high_stop") else List("low_delta", "low_stop"),
      tickerParams.spark,
      tickerParams.trendDataset())

    _generate(
      "correction",
      if (tickerParams.correction.spy) "spy_open_delta" else "benchmark_open_delta",
      if (tickerParams.action == "buy") List("high_delta", "low_stop") else List("low_delta", "high_stop"),
      tickerParams.spark,
      tickerParams.correctionDataset())
  }
}
