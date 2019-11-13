package fiveMinute

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions._


trait OpenDelta {

  private val orderColumn = "date"

  private val dayBarsWindow = Window.partitionBy("date_string").orderBy(orderColumn)

  private val fillPreviousValue = Window.orderBy(orderColumn).rowsBetween(Window.unboundedPreceding, -1)

  private def dayOpen(dataset: Dataset[Row]) = {

    dataset
      .withColumn("date_string", date_format(col(orderColumn), "yyyy-MM-dd"))
      .withColumn("dayOpen", when(row_number.over(dayBarsWindow) === 1, dataset("open")).otherwise(null))
      .withColumn("dayOpen",
        coalesce(col("dayOpen"), last("dayOpen", true).over(fillPreviousValue)))
  }

  def openDelta(targetColumnName: String, dataset: Dataset[Row]) = {

    this.dayOpen(dataset)
      .withColumn(targetColumnName, round(col("close") / col("dayOpen"), 4))
      .repartition(col("date"))
  }
}
