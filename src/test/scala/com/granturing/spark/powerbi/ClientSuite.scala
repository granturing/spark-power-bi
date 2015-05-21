package com.granturing.spark.powerbi

import org.apache.spark.SparkConf
import org.scalatest.FunSuite
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration.Duration

class ClientSuite extends FunSuite {

  val clientConf = ClientConf.fromSparkConf(new SparkConf())
  val client = new Client(clientConf)

  test("client can list datasets") {
    val ds = Await.result(client.getDatasets, clientConf.timeout)

    assert(ds != null)
  }

}
