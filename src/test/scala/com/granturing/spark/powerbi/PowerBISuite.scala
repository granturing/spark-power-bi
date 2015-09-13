/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.granturing.spark.powerbi

import org.apache.spark.sql.{SaveMode, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import scala.concurrent.Await

case class Person(name: String, age: Int, birthday: java.sql.Date, timestamp: java.sql.Timestamp)

class PowerBISuite extends FunSuite with BeforeAndAfterAll {

  val dataset = "PowerBI Spark Test"
  var datasetId: String = _

  val table = "People"
  val tableSchema = Table(
    table, Seq(
      Column("name", "string"),
      Column("age", "Int64"),
      Column("birthday", "Datetime"),
      Column("timestamp", "Datetime")
    ))

  val group = sys.env.get("POWERBI_GROUP")
  val opts = {
    val _opts = Map("dataset" -> dataset, "table" -> table)

    group match {
      case Some(g) => _opts ++ Map("group" -> g)
      case None => _opts
    }
  }

  val conf = new SparkConf().
    setAppName("spark-powerbi-test").
    setMaster("local[1]").
    set("spark.task.maxFailures", "1")
  val clientConf = ClientConf.fromSparkConf(new SparkConf())
  val client: Client = new Client(clientConf)
  val sc: SparkContext = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._

  override  def beforeAll: Unit = {
    val groupId = group match {
      case Some(grp) => {
        val grpOpt = Await.result(client.getGroups, clientConf.timeout).filter(g => grp.equals(g.name)).map(_.id).headOption

        grpOpt match {
          case Some(g) => Some(g)
          case None => sys.error(s"group $grp not found")
        }
      }
      case None => None
    }

    val ds = Await.result(client.getDatasets(groupId), clientConf.timeout)

    datasetId = ds.filter(_.name == dataset).headOption match {
      case Some(d) => {
        Await.result(client.clearTable(d.id, table, groupId), clientConf.timeout)
        Await.result(client.updateTableSchema(d.id, table, tableSchema, groupId), clientConf.timeout)
        d.id
      }
      case None => {
        val result = Await.result(client.createDataset(Schema(dataset, Seq(tableSchema)), groupId), clientConf.timeout)
        result.id
      }
    }
  }

  override def afterAll: Unit = {
    sc.stop()
  }

  test("RDD saves to PowerBI") {
    val data = sc.parallelize(Seq(Person("Joe", 24, new java.sql.Date(1420088400000L), new java.sql.Timestamp(new java.util.Date().getTime))))

    data.saveToPowerBI(dataset, table, group = group)
  }

  test(s"RDD with over ${clientConf.maxPartitions} partitions saves to PowerBI") {
    val data = sc.parallelize(
      0 to clientConf.maxPartitions map { i =>
        Person(s"Person$i", i, new java.sql.Date(1420088400000L), new java.sql.Timestamp(new java.util.Date().getTime))
      },
      clientConf.maxPartitions+1)

    data.saveToPowerBI(dataset, table, group = group)
  }

  test("RDD over batch size saves to PowerBI") {
    val data = sc.parallelize(
      1 to clientConf.batchSize + 1 map { i =>
        Person(s"Person$i", i, new java.sql.Date(1420088400000L), new java.sql.Timestamp(new java.util.Date().getTime))
      }
      , 1)

    data.saveToPowerBI(dataset, table, group = group)
  }

  test("DataFrame saves with overwrite to PowerBI") {
    val data = sc.parallelize(Seq(Person("Joe", 24, new java.sql.Date(1420088400000L), new java.sql.Timestamp(new java.util.Date().getTime)))).toDF

    data.
      write.
      format("com.granturing.spark.powerbi").
      options(opts).
      mode(SaveMode.Overwrite).save
  }

  test("DataFrame saves with append to PowerBI") {
    val data = sc.parallelize(Seq(Person("Joe", 24, new java.sql.Date(1420088400000L), new java.sql.Timestamp(new java.util.Date().getTime)))).toDF

    data.
      write.
      format("com.granturing.spark.powerbi").
      options(opts).
      mode(SaveMode.Append).save
  }

  test("DataFrame save fails if exists") {
    val data = sc.parallelize(Seq(Person("Joe", 24, new java.sql.Date(1420088400000L), new java.sql.Timestamp(new java.util.Date().getTime)))).toDF

    val ex = intercept[RuntimeException] {
      data.
        write.
        format("com.granturing.spark.powerbi").
        options(opts).
        mode(SaveMode.ErrorIfExists).save
    }

    assertResult(ex.getMessage())(s"table $table already exists")
  }
}
