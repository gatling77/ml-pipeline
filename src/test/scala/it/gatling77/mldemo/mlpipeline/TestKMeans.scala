package it.gatling77.mldemo.mlpipeline


import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Created by gatling77 on 3/23/18.
  */

@RunWith(classOf[JUnitRunner])
class TestKMeans extends FunSuite with BeforeAndAfterAll {

  lazy val sut = new Clusterizator("/home/gatling77/dev/mldemo/mlpipeline/src/test/resources/data.csv",3,20)

  test("SUT can be instantiated"){
    val instantiated = try {
      sut
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiated, "Can't instantiate a StackOverflow object")
  }

  test("read raw data reads the expected number of lines"){
    val lines = sut.readRawData()
    assert(lines.count()==3)
  }

}