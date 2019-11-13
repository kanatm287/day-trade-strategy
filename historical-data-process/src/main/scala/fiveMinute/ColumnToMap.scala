package fiveMinute

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions._


trait ColumnToMap {

  private val orderColumn = "date"

  private val dayBarsWindow = Window.partitionBy("date_string").orderBy(orderColumn)

  def columnToMap (sourceColumnName: String,
                dataset: Dataset[Row],
                spark: org.apache.spark.sql.SparkSession) = {

    import spark.implicits._

    dataset
      .withColumn("date_string", date_format(col(orderColumn), "yyyy-MM-dd"))
      .withColumn("values",
        when(row_number.over(dayBarsWindow) === 78, collect_list(sourceColumnName).over(dayBarsWindow))
          .otherwise(null))
      .na.drop()
      .select(Seq("date_string", "values").map(name => col(name)):_*)
      .map(r => (r.getAs[String](0), r.getAs[Seq[Double]](1))).collect().toMap
  }
}
