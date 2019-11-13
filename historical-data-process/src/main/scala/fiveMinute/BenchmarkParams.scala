package fiveMinute

import org.apache.spark.sql.{Dataset, Row}


class BenchmarkParams(val ticker: String,
                      val spark: org.apache.spark.sql.SparkSession) extends Database {

  import spark.implicits._

  private def prop(s:String) = { System.getProperty(s) }

  private var dataset: Dataset[Row] = _

  def getDataset(): Dataset[Row] = {

    if (this.dataset.rdd.partitions.length == 1) {
      this.dataset.repartition($"date")
    } else {
      this.dataset
    }
  }

  def setDataset(newDataset: Dataset[Row]): Unit = {
    this.dataset = newDataset
  }

  if (ticker == "SPY") {
    this.setDataset(initialData("SPY", spark))
  } else {
    this.setDataset(
      initialData(benchmark(ticker, spark), spark))
  }
}
