package fiveMinute

import java.sql.Timestamp
import java.time.Instant

import org.apache.spark.sql.{Dataset, Row}


class PreProcessData() extends LabelData with FilterByBenchmark {

  def generate(tickerParams: TickerParams): Unit = {

//    Label Data

    labelData(tickerParams)

    tickerParams.writeUpdatedDeltas()

    val spark = tickerParams.spark

    val sqlContext = spark.sqlContext

    import sqlContext.implicits._

    tickerParams.setPreProcessParams(
      Seq(1, 2, 3, 4, 5, 6)
        .flatMap{
          sessionPeriod =>
            Seq("trend", "correction")
              .map{
                strategy =>
                    PreProcessParams(
                      Timestamp.from(Instant.now),
                      tickerParams.ticker,
                      sessionPeriod,
                      strategy,
                      None,
                      None,
                      None,
                      None,
                      None,
                      None,
                      None,
                      None,
                      None,
                      None,
                      None,
                      None,
                      None)}}
        .toDS
        .asInstanceOf[Dataset[Row]])

//    Split Strategy

    filterByBenchmark(tickerParams)

//    Remove Outliers

    val possibles = new Possibles(tickerParams)

    possibles.generate()

//    Remove Exceptions
//
    val exceptions = new Exceptions(tickerParams)

    exceptions.generate()

//    Write to database

    tickerParams.writePreProcessParamsToDatabase()
  }
}
