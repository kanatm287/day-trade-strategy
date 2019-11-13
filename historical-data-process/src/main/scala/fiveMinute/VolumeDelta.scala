package fiveMinute

import org.apache.spark.sql.functions._


trait VolumeDelta {

  def volumeDelta(targetColumnName: String, tickerParams: TickerParams) = {

    tickerParams.setDataset(
      tickerParams.dataset()
      .withColumn(targetColumnName,
        round((col("volume") - tickerParams.volumeMin) / (tickerParams.volumeMax - tickerParams.volumeMin),
          3)))
  }
}
