package it.gatling77.mldemo.mlpipeline

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}

/**
  * Created by gatling77 on 3/23/18.
  */
case class UserStats(userId:Int, transactionsPerDay: Double, averageAmount: Double, numberOfMerchant: Int, numberOfPresentationMode: Int, geographicDispersion: Double)

object Clusterizator{
  def main(args: Array[String]): Unit = {
    val file = "src/main/resources/data.csv"; //replace with arg
    val clusterizator = new Clusterizator(file,3,20)

  }

}
class Clusterizator(file: String, numCluster: Int, maxIteration: Int ) {

  //Now MLLIB preferred api is based on DataFrames, so I can't use a SparkContext.
  // I use a SparkSession instead

  lazy val spark: SparkSession = SparkSession.builder.appName("mlpipeline").master("local").getOrCreate()
  import spark.implicits._

  private[mlpipeline] def readRawData():RDD[String] = {
    spark.sparkContext.textFile(file)
  }

  private[mlpipeline] def toUserStats(lines: RDD[String]):DataFrame = {
    lines.map(line=>{
      val data = line.split(";")
      UserStats(
        data(0).toInt,
        data(1).toDouble, //transactionPerDay
        data(2).toDouble, //averageAmount
        data(3).toInt,    //numberOfMerchant
        data(4).toInt,    //numberOfPresentationMode
        data(5).toDouble  //geographicDispersion
      )
    }).toDF()
  }

  def calculateClusters(userStats:DataFrame) ={
        val samples = userStats.drop("userId") // doesn't make sense to cluster based on userId

        val k = new KMeans().setK(numCluster).setMaxIter(maxIteration).setSeed(1l).setFeaturesCol("transactionsPerDay, averageAmount")

  }

}
