package fiveMinute

import java.time.Instant
import java.sql.Timestamp
import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}


class Classify(tickerParams: TickerParams) {

  private def _generate(strategy: String,
                        benchmarkColumnName: String,
                        deltas: List[String]): Unit = {

    implicit def bool2int(b:Boolean) = if (b) 1 else 0

    val bool2int_udf = udf(bool2int _)

    val exceptions = new Exceptions(tickerParams)

    deltas.foreach { delta =>

      (for (i <- 1 until 7 by 1) yield
        tickerParams.dataset()
          .filter(col("strategy") === strategy
            && !array_contains(col(delta + "_exceptions_dates"), col("date"))
            && col("session_period") === i))
        .foreach { e =>

          val sessionPeriod = e.select("session_period").distinct.first().getInt(0)

          val data =
            e.withColumn("label", bool2int_udf(col(delta)))
              .select(
                col("label"),
                col("volume_delta"),
                col("open_delta"),
                col(benchmarkColumnName))

          val assembler = new VectorAssembler()
            .setInputCols(Array("volume_delta", "open_delta", benchmarkColumnName))
            .setOutputCol("features")

          val pipeline = new Pipeline()

          val logisticRegression = new LogisticRegression()
            .setMaxIter(10)

          val logisticRegressionParamGrid = new ParamGridBuilder()
            .baseOn(pipeline.stages -> Array[PipelineStage](assembler, logisticRegression))
            .addGrid(logisticRegression.regParam, Array(10, 1, 0.1, 0.01))
            .build()

          val randomForest = new RandomForestClassifier()
            .setImpurity("entropy")
            .setNumTrees(50)

          val randomForestParamGrid = new ParamGridBuilder()
            .baseOn(pipeline.stages -> Array[PipelineStage](assembler, randomForest))
            .build()

          val layers = Array[Int](3, 6, 2)

          val multilayerPerceptron = new MultilayerPerceptronClassifier()
            .setLayers(layers)
            .setBlockSize(128)
            .setSeed(System.currentTimeMillis)
            .setMaxIter(100)

          val multilayerPerceptronParamGrid = new ParamGridBuilder()
            .baseOn(pipeline.stages -> Array[PipelineStage](assembler, multilayerPerceptron))
            .build()

          val paramGrid = logisticRegressionParamGrid ++ randomForestParamGrid ++ multilayerPerceptronParamGrid

          val trainValidationSplit = new TrainValidationSplit()
            .setEstimator(pipeline)
            .setEvaluator(new BinaryClassificationEvaluator)
            .setEstimatorParamMaps(paramGrid)
            .setTrainRatio(0.75)
            .setParallelism(Runtime.getRuntime().availableProcessors())

          val byteArrayOutputStream = new ByteArrayOutputStream()

          val objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)

          objectOutputStream.writeObject(trainValidationSplit.fit(data))
          objectOutputStream.flush()
          objectOutputStream.close()

          tickerParams.classifyParams_=(
            tickerParams.classifyParams
              .withColumn(delta + "_classifier",
                when(col("session_period") === sessionPeriod && col("strategy") === strategy,
                  byteArrayOutputStream.toByteArray)
                  .otherwise(col(delta + "_classifier"))))
        }
    }
  }

  def generate(): Unit = {

    val sqlContext = tickerParams.spark.sqlContext

    import sqlContext.implicits._

    tickerParams.classifyParams_=(
      Seq(1, 2, 3, 4, 5, 6).flatMap{ sessionPeriod =>
        Seq("trend", "correction").map{strategy =>
          ClassifyParams(Timestamp.from(Instant.now),
            tickerParams.ticker,
            sessionPeriod,
            strategy,
            None,
            None,
            None,
            None)
        }
      }.toDS
        .asInstanceOf[Dataset[Row]])

    _generate(
      "trend",
      if (tickerParams.trend.spy) "spy_open_delta" else "benchmark_open_delta",
      if (tickerParams.action == "buy") List("high_delta", "high_stop") else List("low_delta", "low_stop"))

    _generate(
      "correction",
      if (tickerParams.correction.spy) "spy_open_delta" else "benchmark_open_delta",
      if (tickerParams.action == "buy") List("high_delta", "low_stop") else List("low_delta", "high_stop"))

    tickerParams.writeClassifiers()
  }
}
