import sbt.Keys.scalaVersion

name := "mlpipeline"

version := "1.0"

scalaVersion := "2.11.12"

libraryDependencies ++= {
  val sparkVer = "2.3.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer ,
    "org.apache.spark" %% "spark-sql" % sparkVer ,
    "org.apache.spark" %% "spark-mllib" % sparkVer
  )
}

libraryDependencies ++= {
  val vegasVer = "0.3.11"
  Seq(
    "org.vegas-viz" %% "vegas" % vegasVer ,
    "org.vegas-viz" %% "vegas-spark" % vegasVer
  )
}

libraryDependencies += "junit" % "junit" % "4.10" % "test"
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.5" % "test"
