package fiveMinute

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType}


trait LabelData {

  def labelData(tickerParams: TickerParams) = {

    val action = tickerParams.action

    val trendLimitPercent = tickerParams.trend.limit_price

    val correctionLimitPercent = tickerParams.correction.limit_price

    val trendStopPercent = tickerParams.trend.min_trail_percent

    val correctionStopPercent = tickerParams.correction.min_trail_percent

    val defaultStopPercent = 1.0005

    if (action == "buy") {

      tickerParams.setDataset(
        tickerParams.dataset()
          .withColumn("high_delta",
            when(col("close") > col("prev_day_close"),
              when(col("day_max") / col("close") > trendLimitPercent, true)
                .otherwise(false))
            .otherwise(when(col("day_max") / col("close") > correctionLimitPercent, true)
              .otherwise(false)))
          .withColumn("high_stop",
            when(col("day_max") / col("close") < lit(defaultStopPercent), true)
              .otherwise(false))
          .withColumn("low_delta", lit(null).cast(DoubleType))
          .withColumn("low_stop",
            when(col("close") > col("prev_day_close"),
              when(col("close") / col("day_min") < lit(1 - trendStopPercent + 1),
                true)
                .otherwise(false))
              .otherwise(when(col("close") / col("day_min") <
                lit(1 - correctionStopPercent + 1),
                true)
                .otherwise(false))))
    } else {

      tickerParams.setDataset(
        tickerParams.dataset()
          .withColumn("low_delta",
            when(col("close") < col("prev_day_close"),
              when(col("close") / col("day_min") > 1 - trendLimitPercent + 1, true)
                .otherwise(false))
              .otherwise(when(col("close") / col("day_min") > 1 - correctionLimitPercent + 1,
                true)
              .otherwise(false)))
          .withColumn("low_stop",
            when(col("close") / col("day_min") < defaultStopPercent, true)
              .otherwise(false))
          .withColumn("high_delta", lit(null).cast(DoubleType))
          .withColumn("high_stop",
            when(col("close") < col("prev_day_close"),
              when(col("day_max") / col("close") < trendStopPercent, true)
                .otherwise(false))
              .otherwise(when(col("day_max") / col("close") < correctionStopPercent, true)
                .otherwise(false))))
    }
  }
}
