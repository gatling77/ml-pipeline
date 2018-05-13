package it.gatling77.mldemo.mlpipeline

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Created by gatling77 on 3/23/18.
  */

@RunWith(classOf[JUnitRunner])
class TestFraudDetector extends FunSuite with BeforeAndAfterAll {

  lazy val sut = new FraudDetector("/home/gatling77/dev/mldemo/mlpipeline/src/test/resources/sample_mlpnn.csv",Array("amount","lat","lon"),1)

  test("SUT can be instantiated"){
    val instantiated = try {
      sut
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiated, "Can't instantiate a FraudDetector object")
  }

    test("read user stats"){
    val userStats = sut.readTransactions(sut.file)

    assert(userStats.count()==1020)
  }

  test("Calculate NN"){
    val userStats =sut.readTransactions(sut.file)

    val splits = userStats.randomSplit(Array(0.6, 0.4))
    val trainingData = splits(0)
    val testData = splits(1)

    val model = sut.train(trainingData)
    val predictions = sut.evaluate(testData,model)

    predictions.show(10)
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))

  }
}