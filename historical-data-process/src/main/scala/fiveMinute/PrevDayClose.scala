package fiveMinute

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


trait PrevDayClose {

  private val orderColumn = "date"

  private val dayBarsWindow = Window.partitionBy("date_string").orderBy(orderColumn)

  private val fillPreviousValue = Window.orderBy(orderColumn).rowsBetween(Window.unboundedPreceding, -1)

  def prevDayClose(targetColumnName: String, tickerParams: TickerParams) = {

    tickerParams.setDataset(
      tickerParams.dataset()
        .withColumn("dayClose",
          when(row_number.over(dayBarsWindow) === 78, col("close")).otherwise(null))
        .withColumn(targetColumnName,
          coalesce(col("dayClose"), last("dayClose", true).over(fillPreviousValue)))
        .withColumn(targetColumnName,
          when(col(targetColumnName).isNull, col("dayOpen")).otherwise(col(targetColumnName))))
  }
}
