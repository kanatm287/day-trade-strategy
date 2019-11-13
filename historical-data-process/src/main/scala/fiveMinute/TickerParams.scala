package fiveMinute

import net.liftweb.json._
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions._


class TickerParams(val ticker: String,
                   val spark: org.apache.spark.sql.SparkSession) extends Database {

  import spark.implicits._

  def prop(s:String) = { System.getProperty(s) }

  private var _dataset: Dataset[Row] = _

  def dataset(): Dataset[Row] = {

    if (this._dataset.rdd.partitions.length == 1) {
      this._dataset.repartition($"date")
    } else {
      this._dataset
    }
  }

  def setDataset(newDataset: Dataset[Row]): Unit = { this._dataset = newDataset }

  private var _trendDataset: Dataset[Row] = _

  def trendDataset(): Dataset[Row] = {

    val partitions = this._trendDataset.rdd.partitions.length

    if (partitions == 1 || partitions > 200) {
      this._trendDataset.repartition($"date")
    } else {
      this._trendDataset
    }
  }

  def setTrendDataset(newDataset: Dataset[Row]): Unit = { this._trendDataset = newDataset }

  private var _correctionDataset: Dataset[Row] = _

  def correctionDataset(): Dataset[Row] = {

    if (this._correctionDataset.rdd.partitions.length == 1) {
      this._correctionDataset.repartition($"date")
    } else {
      this._correctionDataset
    }
  }

  def setCorrectionDataset(newDataset: Dataset[Row]): Unit = { this._correctionDataset = newDataset }

  private var _preProcessParams: Dataset[Row] = _

  def preProcessParams(): Dataset[Row] = { this._preProcessParams }

  def setPreProcessParams(params: Dataset[Row]): Unit = { this._preProcessParams = params }

  private[this] var _classifyParams: Dataset[Row] = _

  def classifyParams: Dataset[Row] = _classifyParams

  def classifyParams_=(value: Dataset[Row]): Unit = { _classifyParams = value }

  private val rawTestParams = testParams(ticker, spark)

  val action = rawTestParams.select("action").distinct().first().get(0)

  val volumeMax = rawTestParams.select("volume_max").distinct().first().get(0).asInstanceOf[Int]

  val volumeMin = rawTestParams.select("volume_min").distinct().first().get(0).asInstanceOf[Int]

  implicit val formats = DefaultFormats

  val trend = parse(rawTestParams
    .where("strategy = 'trend'")
    .select("technical_params")
    .first()
    .get(0)
    .asInstanceOf[String])
    .extract[TestParams]

  val correction = parse(rawTestParams
    .where("strategy = 'correction'")
    .select("technical_params")
    .first()
    .get(0)
    .asInstanceOf[String])
    .extract[TestParams]

  def loadInitialData(): Unit = { this._dataset = initialData(ticker, spark) }

  def loadPreparedData(): Unit = { this._dataset = preparedData(ticker, spark) }

  def loadPrePredictData(): Unit = { this._dataset = prePredictData(ticker, spark) }

  def loadPreClassifyData(): Unit = { this._dataset = preClassifyData(ticker, spark) }

  def writePreparedData(): Unit = {

    if (action == "buy") {
      writeDatasetToDatabase(
        "public.test_five_minute_data",
        spark,
        this._dataset
          .withColumn("ticker", lit(ticker))
          .withColumn("strategy",
            when(col("close") > col("prev_day_close"), lit("trend"))
              .otherwise(lit("correction")))
          .drop("open", "high", "low", "close", "volume", "dayClose", "date_string", "dayOpen"))
    } else {
      writeDatasetToDatabase(
        "public.test_five_minute_data",
        spark,
        this._dataset
          .withColumn("ticker", lit(ticker))
          .withColumn("strategy",
            when(col("close") < col("prev_day_close"), lit("trend"))
              .otherwise(lit("correction")))
          .drop("open", "high", "low", "close", "volume", "dayClose", "date_string", "dayOpen"))
    }
  }

  def writeUpdatedDeltas():
  Unit = updateDeltas(_dataset)

  def writePreProcessParamsToDatabase():
  Unit = writeDatasetToDatabase("public.ml_pre_process_params", spark, _preProcessParams)

  def loadPreProcessParamsFromDb(): Unit = this._preProcessParams = preProcessParams(ticker, spark)

  def writeClassifiers():
  Unit = writeDatasetToDatabase("public.ml_classifiers", spark, _classifyParams)

  def loadClassifiers(): Unit = this._classifyParams = classifiers(ticker, spark)
}
