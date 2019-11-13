package fiveMinute

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


trait HighLowAverage {

  private val orderColumn = "date"

  private val orderByDate = Window.orderBy(orderColumn)

  private val sixBarWindow = Window.orderBy(orderColumn).rowsBetween((-1 * 6) + 1, 0)

  def highLowAverage(targetColumnName: String, benchmarkParams: BenchmarkParams) = {

    val maxMinColumns = Array(col("maxHigh"), col("minLow"))

    val averageFunc = maxMinColumns.foldLeft(lit(0)){(x, y) => x+y}/maxMinColumns.length

    benchmarkParams.setDataset(
      benchmarkParams.getDataset()
        .withColumn("maxHigh", max(benchmarkParams.getDataset()("high")).over(sixBarWindow))
        .withColumn("minLow", min(benchmarkParams.getDataset()("low")).over(sixBarWindow))
        .withColumn(colName = "highLowAverage", averageFunc)
        .withColumn("row_nr", row_number().over(orderByDate) - lit(1))
        .select(Seq("row_nr", orderColumn, "open", "close", targetColumnName).map(name => col(name)):_*))
  }
}
