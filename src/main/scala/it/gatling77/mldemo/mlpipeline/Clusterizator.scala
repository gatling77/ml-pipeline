package it.gatling77.mldemo.mlpipeline

import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler

/**
  * Created by gatling77 on 3/23/18.
  */
case class UserStats(userId:Int, transactionsPerDay: Double, averageAmount: Double, numberOfMerchant: Int, numberOfPresentationMode: Int, geographicDispersion: Double)

object Clusterizator{
  def main(args: Array[String]): Unit = {
    val file = "src/main/resources/large_dataset.csv"; //replace with arg
    val clusterizator = new Clusterizator(file,3,20,Array("transactionsPerDay","averageAmount","geographicDispersion"))

  }

}
class Clusterizator(val file: String, numCluster: Int, maxIteration: Int, features: Array[String]) {

  //Now MLLIB preferred api is based on DataFrames, so I can't use a SparkContext.
  // I use a SparkSession instead

  def calculateClusters():DataFrame = {
    val data=readUserStats(file)
    calculateClusters(data)
  }

  lazy val spark: SparkSession = SparkSession.builder.appName("mlpipeline").master("local").getOrCreate()
  import spark.implicits._


  private[mlpipeline] def readUserStats(file:String):DataFrame = {
    spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true").load(file)
  }

  private[mlpipeline] def calculateClusters(userStats:DataFrame):DataFrame={
        val vectorAssembler = new VectorAssembler().setInputCols(features).setOutputCol("features")
        val kmeans = new KMeans().setK(numCluster).setMaxIter(maxIteration).setSeed(1l).setFeaturesCol("features").setPredictionCol("cluster")

        val pipeline =new Pipeline().setStages(Array(vectorAssembler,kmeans))

        val model = pipeline.fit(userStats)
        model.transform(userStats)
  }

}
