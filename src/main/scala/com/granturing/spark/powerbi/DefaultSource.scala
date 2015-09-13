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

import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider}
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration.Duration

class DefaultSource extends CreatableRelationProvider with PowerBISink {

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {

    val conf = ClientConf.fromSparkConf(sqlContext.sparkContext.getConf)
    implicit val client = new Client(conf)

    val dataset = parameters.getOrElse("dataset", sys.error("'dataset' must be specified"))
    val table = parameters.getOrElse("table", sys.error("'table' must be specified"))
    val batchSize = parameters.getOrElse("batchSize", conf.batchSize.toString).toInt
    val group = parameters.get("group")

    val step = for {
      groupId <- getGroupId(group)
      ds <- getOrCreateDataset(mode, groupId, dataset, table, data.schema)
    } yield (groupId, ds)

    val result = step map { case (groupId, ds) =>
      val fields = data.schema.fieldNames.zipWithIndex
      val _conf = conf
      val _token = Some(client.currentToken)
      val _table = table
      val _batchSize = batchSize

      val coalesced = data.rdd.partitions.size > _conf.maxPartitions match {
        case true => data.coalesce(_conf.maxPartitions)
        case false => data
      }

      coalesced foreachPartition { p =>
        val rows = p map { r =>
          fields map { case(name, index) => (name -> r(index)) } toMap
        } toSeq

        val _client = new Client(_conf, _token)

        val submit = rows.
          sliding(_batchSize, _batchSize).
          foldLeft(future()) { (fAccum, batch) =>
          fAccum flatMap { _ => _client.addRows(ds.id, _table, batch, groupId) } }

        submit.onComplete { _ => _client.shutdown() }

        Await.result(submit, _conf.timeout)
      }
    }

    result.onComplete { _ => client.shutdown() }

    Await.result(result, Duration.Inf)

    new BaseRelation {
      val sqlContext = data.sqlContext

      val schema = data.schema
    }
  }

}
