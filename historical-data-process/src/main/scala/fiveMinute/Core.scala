package fiveMinute

import org.apache.spark.sql.SparkSession


class Core {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Historical data processing")
    .config("spark.executor.memory", "2g")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  def stopSparkSession(): Unit = { spark.stop() }

  def prepareData(ticker: String) = {

    //    Ticker Block

    val tickerParams = new TickerParams(ticker, spark)

    tickerParams.loadInitialData()

    val tickerData = new PrepareData()

    tickerData.generate(tickerParams)

    //    Benchmark Block

    val benchmarkData = new BenchmarkData()

    val benchmarkParams = new BenchmarkParams(ticker, spark)

    val spyParams = new BenchmarkParams("SPY", spark)

    val benchmark = benchmarkData.generate("benchmark", benchmarkParams)

    val spy = benchmarkData.generate("spy", spyParams)

    tickerParams.setDataset(
      tickerParams.dataset()
        .join(benchmark, Seq("date"), "inner")
        .join(spy, Seq("date"), "inner"))

    tickerParams.writePreparedData()
  }

  def preProcessData(ticker: String): Unit = {

    val tickerParams = new TickerParams(ticker, spark)

    tickerParams.loadPreparedData()

    val preProcessData = new PreProcessData()

    preProcessData.generate(tickerParams)

  }

  def classifyData(ticker: String): Unit = {

    val tickerParams = new TickerParams(ticker, spark)

    tickerParams.loadPreClassifyData()

    val classify = new Classify(tickerParams)

    classify.generate()

  }

  def predictData(ticker: String): Unit = {

    val tickerParams = new TickerParams(ticker, spark)

    tickerParams.loadClassifiers()

    tickerParams.loadPrePredictData()

    val predict = new Predict(tickerParams)

    predict.generate()

  }
}
