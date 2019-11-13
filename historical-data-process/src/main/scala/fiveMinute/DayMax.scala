package fiveMinute

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


trait DayMax extends ColumnToMap {

  private val orderColumn = "date"

  private val dayBarsWindow = Window.partitionBy("date_string").orderBy(orderColumn)

  def dayMax(targetColumnName: String, tickerParams: TickerParams) = {

    val highs = columnToMap("high", tickerParams.dataset(), tickerParams.spark)

    val getMaxValueUdf = udf ((rowNumer: Int, date: String) => {
      highs(date).slice(rowNumer, highs(date).size).max
    })
    tickerParams.setDataset(
      tickerParams.dataset()
        .withColumn(targetColumnName,
          getMaxValueUdf(row_number().over(dayBarsWindow) - lit(1), col("date_string"))))
  }
}
