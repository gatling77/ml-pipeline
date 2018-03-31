package it.gatling77.mldemo.mlpipeline


import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Created by gatling77 on 3/23/18.
  */

@RunWith(classOf[JUnitRunner])
class TestClusterizator extends FunSuite with BeforeAndAfterAll {

  lazy val sut = new Clusterizator("/home/gatling77/dev/mldemo/mlpipeline/src/test/resources/data.csv",3,20,Array("transactionsPerDay","averageAmount","geographicDispersion"))

  test("SUT can be instantiated"){
    val instantiated = try {
      sut
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiated, "Can't instantiate a Clusterizator object")
  }

    test("read user stats"){
    val userStats = sut.readUserStats(sut.file)

    assert(userStats.count()==3)
  }

  test("Calculate clusters"){
    val userStats =sut.readUserStats(sut.file)
    sut.calculateClusters(userStats)
  }
}