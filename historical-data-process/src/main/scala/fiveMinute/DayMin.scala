package fiveMinute

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


trait DayMin extends ColumnToMap {

  private val orderColumn = "date"

  private val dayBarsWindow = Window.partitionBy("date_string").orderBy(orderColumn)

  def dayMin(targetColumnName: String, tickerParams: TickerParams) = {

    val lows = columnToMap("low", tickerParams.dataset(), tickerParams.spark)

    val getMinValueUdf = udf ((rowNumer: Int, date: String) => {
      lows(date).slice(rowNumer, lows(date).size).min
    })
    tickerParams.setDataset(
      tickerParams.dataset()
        .withColumn(targetColumnName,
          getMinValueUdf(row_number().over(dayBarsWindow) - lit(1), col("date_string"))))
  }
}
