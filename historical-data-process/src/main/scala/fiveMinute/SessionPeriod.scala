package fiveMinute

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


trait SessionPeriod {

  private val orderColumn = "date"

  private val dayBarsWindow = Window.partitionBy("date_string").orderBy(orderColumn)

  def sessionPeriod(targetColumnName: String, tickerParams: TickerParams) = {

    tickerParams.setDataset(
      tickerParams.dataset()
        .withColumn("date_string", date_format(col(orderColumn), "yyyy-MM-dd"))
        .withColumn(targetColumnName,
          when(row_number().over(dayBarsWindow) <= 13, lit(1))
            .otherwise(when(row_number().over(dayBarsWindow) <= 26, lit(2))
              .otherwise(when(row_number().over(dayBarsWindow) <= 39, lit(3))
                .otherwise(when(row_number().over(dayBarsWindow) <= 52, lit(4))
                  .otherwise(when(row_number().over(dayBarsWindow) <= 65, lit(5))
                    .otherwise(lit(6))))))))
  }
}
