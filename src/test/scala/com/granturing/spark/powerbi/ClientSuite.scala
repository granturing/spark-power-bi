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

import org.apache.spark.SparkConf
import org.scalatest.{BeforeAndAfterAll, Matchers, FunSuite}
import scala.concurrent.Await

class ClientSuite extends FunSuite with Matchers with BeforeAndAfterAll {

  val clientConf = ClientConf.fromSparkConf(new SparkConf())
  val client = new Client(clientConf)

  val dataset = "PowerBI Spark Test"
  var datasetId: String = _
  val group = sys.env.get("POWERBI_GROUP")
  var groupId: Option[String] = None
  val table = "People"
  val tableSchema = Table(
    table, Seq(
      Column("name", "string"),
      Column("age", "Int64"),
      Column("birthday", "Datetime"),
      Column("timestamp", "Datetime")
    ))

  override def beforeAll = {
    groupId = group match {
      case Some(grp) => {
        val grpOpt = Await.result(client.getGroups, clientConf.timeout).filter(g => grp.equals(g.name)).map(_.id).headOption

        grpOpt match {
          case Some(g) => Some(g)
          case None => sys.error(s"group $grp not found")
        }
      }
      case None => None
    }
  }

  test("client can list groups") {
    val groups = Await.result(client.getGroups, clientConf.timeout)

    groups should not be null
  }

  test("client can list datasets") {
    val ds = Await.result(client.getDatasets(groupId), clientConf.timeout)

    ds should not be null
  }

}
