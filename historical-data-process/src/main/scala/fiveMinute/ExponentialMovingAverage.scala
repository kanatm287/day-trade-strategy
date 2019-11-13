package fiveMinute

import org.apache.spark.sql.functions._


trait ExponentialMovingAverage {

  private val orderColumn = "date"

  def exponentialMovingAverage(windowSize: Int,
                               sourceColumnName: String,
                               targetColumnName: String,
                               benchmarkParams: BenchmarkParams) = {

    val spark = benchmarkParams.spark

    val alpha = 2.0 / (windowSize + 1)

    import spark.implicits._

    val sourceColumnList = benchmarkParams.getDataset()
      .orderBy(orderColumn).select(sourceColumnName).map(r => r.getAs[Double](0)).collect()

    val emaUDF = udf((rowNumber: Int) => {
      val adjustedWeights = (0 until rowNumber + 1).foldLeft(new Array[Double](rowNumber + 1)) {
        (accumulator, index) => accumulator(index) = Math.pow(1 - alpha, rowNumber - index);
          accumulator
      }
      (adjustedWeights, sourceColumnList.slice(0, rowNumber + 1)).zipped.map(_ * _).sum / adjustedWeights.sum
    })

    benchmarkParams.setDataset(
      benchmarkParams.getDataset()
        .withColumn(targetColumnName, emaUDF(col("row_nr"))))
  }
}
