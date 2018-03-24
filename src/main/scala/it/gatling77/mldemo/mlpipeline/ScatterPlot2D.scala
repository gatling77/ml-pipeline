package it.gatling77.mldemo.mlpipeline


import org.apache.spark.sql.DataFrame
import vegas._
import vegas.render.WindowRenderer._
import vegas.sparkExt._

/**
  * Created by gatling77 on 3/24/18.
  */
class ScatterPlot2D(title: String, data: DataFrame, x:String, y:String) {

  val plot = Vegas(title)
    .withDataFrame(data).mark(Point).encodeX(x,Quantitative)
    .encodeY(y,Quantitative)

  def show(): Unit ={
    plot.window.show
  }
}

object Scatter{
  val file = "src/main/resources/data.csv"; //replace with arg
  val clusterizator = new Clusterizator(file,3,20)

  def main(args: Array[String]): Unit ={
    val data = clusterizator.toUserStats(clusterizator.readRawData())
    val plot= new ScatterPlot2D("test",data,"transactionsPerDay","averageAmount")
    plot.show()
  }

}
