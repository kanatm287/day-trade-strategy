package fiveMinute

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


trait MovingAverage {

  private val orderColumn = "date"

  def movingAverage(period: Int,
               sourceColumnName: String,
               targetColumnName: String,
               benchmarkParams: BenchmarkParams) = {

    val movingAverageWindow = Window.orderBy(this.orderColumn).rowsBetween((-1 * period) + 1, 0)

    benchmarkParams.setDataset(
      benchmarkParams.getDataset()
        .withColumn(colName = targetColumnName,
          avg(benchmarkParams.getDataset()(sourceColumnName)).over(movingAverageWindow))
    )
  }
}
