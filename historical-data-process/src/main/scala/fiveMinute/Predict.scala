package fiveMinute

import java.io.{ByteArrayInputStream, ObjectInputStream}

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.tuning.TrainValidationSplitModel
import org.apache.spark.sql.types._


class Predict(tickerParams: TickerParams) {

  val spark = tickerParams.spark

  import spark.implicits._

  private val orderColumn = "date"

  private val dayBarsWindow = Window.partitionBy("date_string").orderBy(orderColumn)

  private val fillPreviousValue = Window.orderBy(orderColumn).rowsBetween(Window.unboundedPreceding, -1)

  private val trendBenchmarkColumnName = if (tickerParams.trend.spy) "spy_open_delta" else "benchmark_open_delta"

  private val correctionBenchmarkColumnName =
    if (tickerParams.correction.spy) "spy_open_delta" else "benchmark_open_delta"

  private def loadModel(byteArray: Array[Byte]): TrainValidationSplitModel = {

    val byteArrayInputStream = new ByteArrayInputStream(byteArray)
    val objectInputStream = new ObjectInputStream(byteArrayInputStream)

    objectInputStream.readObject.asInstanceOf[TrainValidationSplitModel]
  }

  private def predict(dataset: Dataset[Row],
                      sessionPeriod: Int,
                      strategy: String,
                      columnName: String): Dataset[Row] = {

    implicit def doubleToBool(d:Double) = if (d > 0) true else false

    val doubleToBoolUdf = udf(doubleToBool _)

    loadModel(tickerParams.classifyParams
    .filter(col("session_period") === sessionPeriod && col("strategy") === strategy)
    .select(columnName + "_classifier").first().getAs[Array[Byte]](0))
    .transform(dataset)
    .withColumn(columnName, doubleToBoolUdf(col("prediction")))
    .withColumn(columnName,
      when(
        array_contains(col(columnName + "_exceptions_dates"), col("date")) ||
          !array_contains(col("benchmark_possibles_dates"), col("date")) ||
          !array_contains(col("non_outliers_possibles_dates"), col("date")), null)
        .otherwise(col(columnName)))
    .drop("features", "rawPrediction", "probability", "prediction")
  }

  private def _generate(accumulator: Dataset[Row],
                        dataset: Dataset[Row]): Dataset[Row] = {

    val sessionPeriod = dataset.select("session_period").distinct.first().getInt(0)

    val strategy = dataset.select("strategy").distinct.first().getString(0)

    val columnNames =  if (strategy == "trend") {
      if (tickerParams.action == "buy") {
        Seq("date", "high_delta", "high_stop", "low_delta", "low_stop")
      } else {
        Seq("date", "low_delta", "low_stop", "high_delta", "high_stop")
      }
    } else {
      if (tickerParams.action == "buy") {
        Seq("date", "high_delta", "low_stop", "low_delta", "high_stop")
      } else {
        Seq("date", "low_delta", "high_stop", "high_delta", "low_stop")
      }
    }

    accumulator.union(
      predict(dataset, sessionPeriod, strategy, columnNames(1))
        .join(predict(dataset, sessionPeriod, strategy, columnNames(2)),
          Seq("date", "ticker"),
          "inner")
        .withColumn(columnNames(3), lit(null: BooleanType))
        .withColumn(columnNames(4), lit(null: BooleanType))
        .select(Seq.empty[PredictDeltasParams].toDS().columns.map(col _) : _*))
  }

  def generate(): Unit = {

    val predictParams = List(1, 2, 3, 4, 5, 6)
      .flatMap{
        sessionPeriod =>
          List("trend", "correction").map{
            strategy =>
              tickerParams.dataset()
                .filter(col("session_period") === sessionPeriod && col("strategy") === strategy)}}

    val columnNames =
      if (tickerParams.action == "buy") {
        Seq("high_delta", "high_stop", "low_stop")
      } else {
        Seq("low_delta", "low_stop", "high_stop")
      }

    tickerParams.setDataset(
      predictParams
        .fold(Seq.empty[PredictDeltasParams].toDS().asInstanceOf[Dataset[Row]])
        {(accumulator, dataset) => _generate(accumulator, dataset)}
        .withColumn("date_string", date_format(col(orderColumn), "yyyy-MM-dd"))
        .withColumn(columnNames(0), when(row_number.over(dayBarsWindow) === 1, false)
          .otherwise(col(columnNames(0))))
        .withColumn(columnNames(0), coalesce(col(columnNames(0)),
          last(columnNames(0), true).over(fillPreviousValue)))
        .withColumn(columnNames(1), when(row_number.over(dayBarsWindow) === 1, false)
          .otherwise(col(columnNames(1))))
        .withColumn(columnNames(1), coalesce(col(columnNames(0)),
          last(columnNames(1), true).over(fillPreviousValue)))
        .withColumn(columnNames(2), when(row_number.over(dayBarsWindow) === 1, false)
          .otherwise(col(columnNames(2))))
        .withColumn(columnNames(2), coalesce(col(columnNames(0)),
          last(columnNames(2), true).over(fillPreviousValue))))

    tickerParams.writeUpdatedDeltas()
  }
}
