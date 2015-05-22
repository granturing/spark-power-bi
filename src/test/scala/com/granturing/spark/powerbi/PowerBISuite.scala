package com.granturing.spark.powerbi

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
import scala.concurrent.Await
import scala.concurrent.duration._

case class Person(name: String, age: Int)

class PowerBISuite extends FunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  val dataset = "PowerBI Spark Test"
  var datasetId: String = _
  val table = "People"
  val tableSchema = Table(table, Seq(Column("name", "string"), Column("age", "Int64")))

  var client: Client = _
  var sc: SparkContext = _

  override  def beforeAll: Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("PowerBI Tests")
    val clientConf = ClientConf.fromSparkConf(conf)

    client = new Client(clientConf)

    val ds = Await.result(client.getDatasets, Duration.Inf)

    datasetId = ds.filter(_.name == dataset).headOption match {
      case Some(d) => {
        Await.result(client.updateTableSchema(d.id, table, tableSchema), Duration.Inf)
        d.id
      }
      case None => {
        val result = Await.result(client.createDataset(Schema(dataset, Seq(tableSchema))), Duration.Inf)
        result.id
      }
    }

    sc = new SparkContext(conf)
  }

  override def afterAll: Unit = {
    sc.stop()
  }

  override def afterEach: Unit = {
    Await.result(client.clearTable(datasetId, table), Duration.Inf)
  }

  test("RDD saves to PowerBI") {
    val data = sc.parallelize(Seq(Person("Joe", 24)))

    data.saveToPowerBI(dataset, table)
  }

  test(s"RDD with over ${ClientConf.MAX_PARTITIONS} partitions saves to PowerBI") {
    val list = 0 to ClientConf.MAX_PARTITIONS map { i => Person(s"Person$i", i) }
    val data = sc.parallelize(list, 6)

    data.saveToPowerBI(dataset, table)
  }

  test("RDD over batch size saves to PowerBI") {
    val list = 1 to ClientConf.BATCH_SIZE + 1 map { i => Person(s"Person$i", i) }
    val data = sc.parallelize(list, 1)

    data.saveToPowerBI(dataset, table)
  }

}
