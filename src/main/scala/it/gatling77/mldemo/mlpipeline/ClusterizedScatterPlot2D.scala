package it.gatling77.mldemo.mlpipeline


import java.io.{File, PrintWriter}

import org.apache.spark.sql.DataFrame
import vegas._
import vegas.render.WindowRenderer._
import vegas.sparkExt._

/**
  * Created by gatling77 on 3/24/18.
  */
class ClusterizedScatterPlot2D(title: String, data: DataFrame, x:String, y:String, encodeColor:String) {

  val plot = Vegas(title,height = 1024d,width=1024d)
    .withDataFrame(data).mark(Point)
    .encodeColor(encodeColor, Nominal)
    .encodeX(x,Quantitative)
    .encodeY(y,Quantitative)

  def show(): Unit ={
    plot.window.show
  }

  def json():String={
    plot.toJson
  }
}

object Scatter{
  val file = "src/main/resources/large_dataset.csv"; //replace with arg
  val txVsAmountImage = "target/txVsAmount.json";
  val txVsGeoImage = "target/txVsGeo.json";
  val clusterizator = new Clusterizator(file,3,20,Array("transactionsPerDay","averageAmount","geographicDispersion"))

  def main(args: Array[String]): Unit ={
    val data = clusterizator.calculateClusters()
    val txVsAmount= new ClusterizedScatterPlot2D("test",data,"transactionsPerDay","averageAmount","cluster")
    val txVsGeo= new ClusterizedScatterPlot2D("test",data,"transactionsPerDay","geographicDispersion","cluster")


    writeToFile(txVsAmount,txVsAmountImage)
    writeToFile(txVsGeo,txVsGeoImage)

  }

  def writeToFile(plot:ClusterizedScatterPlot2D, file:String): Unit ={
    val image:PrintWriter = new PrintWriter(new File(file))
    try{
      image.println(plot.json())
    }finally{
      image.close()
    }
  }

}
