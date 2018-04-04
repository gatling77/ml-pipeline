package it.gatling77.mldemo.mlpipeline

import org.apache.spark.ml.{Pipeline, PipelineModel, Transformer, UnaryTransformer}
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.feature.{Tokenizer, VectorAssembler}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{DataType, DoubleType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object FraudDetector{
  def main(args: Array[String]): Unit = {
    val fraudDetector = new FraudDetector("",Array[String]("amount","lat","lon"))

  }
}
/**
  * Created by gatling77 on 3/31/18.
  */
class FraudDetector(val file: String, val features:Array[String]) {

  lazy val spark: SparkSession = SparkSession.builder.appName("mlpipeline").master("local").getOrCreate()
  import spark.implicits._


  private[mlpipeline] def readTransactions(file:String) : DataFrame={
      spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true").load(file)
  }



  private[mlpipeline] def train(transactions: DataFrame): PipelineModel = {
    // it's usually a good starting point to start with a single layer with two times the number of input.
    // please note that the output layer must be at lest 2, since the output uses OneHotEncoder
    val layers=Array[Int](features.length,features.length*2,2)


    val featuresAssembler = new VectorAssembler().setInputCols(features).setOutputCol("features")
    val boolToIn = new BoolToIn().setInputCol("isFraud").setOutputCol("label")
    //val labelAssembler = new VectorAssembler().setInputCols("labelCol").setOutputCol("label")

    val mlpnn = new MultilayerPerceptronClassifier()
        .setLayers(layers)
        .setFeaturesCol("features")
        .setLabelCol("label")
        .setPredictionCol("prediction")
        .setBlockSize(128)
        .setSeed(1234L)
        .setMaxIter(100)

    val pipeline =new Pipeline().setStages(Array(featuresAssembler,boolToIn,mlpnn))

    pipeline.fit(transactions)

  }


  private[mlpipeline] def evaluate(data: DataFrame, mlpnn: PipelineModel):DataFrame={
    mlpnn.transform(data)
  }

  class BoolToIn(override val uid: String) extends UnaryTransformer[Boolean,Double,BoolToIn]{


    override protected def createTransformFunc: (Boolean) => Double = {
      (b: Boolean) => {
        if (b) 1d else 0d
      }
    }

    override protected def outputDataType: DataType = DoubleType

    def this() = this(Identifiable.randomUID("BoolToInt"))
  }
}
