package it.gatling77.mldemo.mlpipeline

import java.io.File

import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler



/**
  * Created by gatling77 on 3/23/18.
  */
case class UserStats(userId:Int, transactionsPerDay: Double, averageAmount: Double, numberOfMerchant: Int, numberOfPresentationMode: Int, geographicDispersion: Double)

object Clusterizator{
  def main(args: Array[String]): Unit = {
    if (args.length < 2){
      throw new IllegalArgumentException("too few parameters")
    }

    val aggregatedIn = args(0)
    val transactionIn = args(1)
    val clustersNumber = args(2).toInt

    val out = args(3)
    val txVsAmountImage = args(4)
    val txVsGeoImage = args(5)

    del(out)
    del(txVsAmountImage)
    del(txVsGeoImage)

    println("Starting k-means with %clusters clusters")
    val clusterizator = new Clusterizator(aggregatedIn,clustersNumber,20,Array("transactionsPerDay","averageAmount","geographicDispersion"))
    val clusters = clusterizator.calculateClusters().drop("numberOfMerchant","numberOfPresentationMode")

    new ClusterizedScatterPlot2D("test",clusters,"transactionsPerDay","averageAmount","cluster").writeToFile(txVsAmountImage)
    new ClusterizedScatterPlot2D("test",clusters,"transactionsPerDay","geographicDispersion","cluster").writeToFile(txVsGeoImage)


    val clusterizedTransactions = clusterizator.clusterizeTransactions(transactionIn, clusters)

    clusterizedTransactions.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save(out)

    clusterizator.stop()
  }

  def del(filename: String): Unit = {

    def delete(file:File){
      if (file.isDirectory)
        file.listFiles.foreach(delete)
      if (file.exists && !file.delete)
        throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
    }
    delete(new File(filename))
  }
}
class Clusterizator(val file: String, numCluster: Int, maxIteration: Int, features: Array[String]) {

  //Now MLLIB preferred api is based on DataFrames, so I can't use a SparkContext.
  // I use a SparkSession instead

  def calculateClusters():DataFrame = {
    val data=readCSV(file)
    calculateClusters(data)
  }

  def stop(): Unit ={
    spark.stop()
  }
  lazy val spark: SparkSession = SparkSession.builder.appName("mlpipeline").master("local").getOrCreate()
  import spark.implicits._


  private[mlpipeline] def readCSV(file:String):DataFrame = {
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
        model.transform(userStats).drop("features")
  }

  // classifies every transaction with the cluster
  def clusterizeTransactions(transactionFile:String, clusters:DataFrame)={
      val transactions= readCSV(transactionFile)
      clusters.join(transactions,$"userId"===$"user").drop("user")
  }
}
