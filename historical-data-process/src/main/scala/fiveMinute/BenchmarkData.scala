package fiveMinute


class BenchmarkData() extends HighLowAverage with MovingAverage with ExponentialMovingAverage with OpenDelta {

  def generate(ticker: String, benchmarkParams: BenchmarkParams) = {

//    High, Low 6 Bar Average

    highLowAverage("highLowAverage", benchmarkParams)

//    Fast Moving Average

    movingAverage(18, "highLowAverage", "fastSma", benchmarkParams)

//    Slow Moving Average

    movingAverage(204, "highLowAverage", "slowSma",benchmarkParams)

//    Fast Exponential Moving Average

    exponentialMovingAverage(
      126, "highLowAverage", "fastEma", benchmarkParams)

//    Slow Exponential Moving Average

    exponentialMovingAverage(
      330, "highLowAverage", "slowEma", benchmarkParams)

//    Benchmark Difference

    benchmarkParams.setDataset(
      benchmarkParams.getDataset()
        .withColumn(ticker + "_difference",
          benchmarkParams.getDataset()("fastSma") - benchmarkParams.getDataset()("slowSma") -
          (benchmarkParams.getDataset()("fastEma") - benchmarkParams.getDataset()("slowEma")))
        .drop("fastSma", "slowSma","fastEma", "slowEma", "row_nr", "highLowAverage"))

//    Open Delta

    openDelta(ticker + "_open_delta", benchmarkParams.getDataset())
    .drop("open", "close", "date_string", "dayOpen")
  }
}
