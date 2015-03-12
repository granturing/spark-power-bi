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
package com.granturing.spark

import java.util.Date
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.{DataType, StructType, SchemaRDD}
import org.apache.spark.streaming.dstream.DStream
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration._
import scala.concurrent.{ future, promise }
import scala.reflect.runtime.universe._

package object powerbi {

  /**
   * @define BATCH_SIZE 10000
   */
  private[powerbi] trait PowerBISink extends Serializable {

    protected val conf: ClientConf

    @transient protected lazy val client = new Client(conf)

    protected def getOrCreateDataset[A <: Product: TypeTag](dataset: String, table: String, tag: TypeTag[A]) = {
      val result = promise[Dataset]

      client.getDatasets map { list =>
        val dsOpt = list.filter(d => dataset.equals(d.name)).headOption

        dsOpt match {
          case Some(d) => result.success(d)
          case None => client.createDataset(Schema(dataset, Seq(Table(table, schemaFor)))) map { result.success(_) }
        }
      }

      result.future
    }

    protected def getOrCreateDataset(dataset: String, table: String, schema: StructType) = {
      val result = promise[Dataset]

      client.getDatasets map { list =>
        val dsOpt = list.filter(d => dataset.equals(d.name)).headOption

        dsOpt match {
          case Some(d) => result.success(d)
          case None => client.createDataset(Schema(dataset, Seq(Table(table, schemaFor(schema))))) map { result.success(_) }
        }
      }

      result.future
    }

    protected def schemaFor[A <: Product: TypeTag](implicit tag: TypeTag[A]) = {
      val values = tag.tpe.members.collect { case m:MethodSymbol if m.isCaseAccessor => m }
      val columns = values.map(v => Column(v.name.toString, typeToBIType(v.typeSignature.baseType(v.typeSignature.typeSymbol)))).toSeq

      columns
    }

    protected def schemaFor(schema: StructType) = {
      val columns = schema.fields.map(f => Column(f.name, sparkTypeToBIType(f.dataType)))

      columns
    }

    protected def sparkTypeToBIType(myType: DataType) = myType match {
      case _: IntegralType => "Int64"
      case _: FractionalType => "Double"
      case StringType => "String"
      case BooleanType => "Boolean"
      case TimestampType | DateType => "Datetime"
      case _ => throw new Exception(s"Unsupported type $myType")
    }

    protected def typeToBIType(myType: Type) = myType match {
      case t if t =:= typeOf[Byte] => "Int64"
      case t if t =:= typeOf[Short] => "Int64"
      case t if t =:= typeOf[Int] => "Int64"
      case t if t =:= typeOf[Integer] => "Int64"
      case t if t =:= typeOf[Long] => "Int64"
      case t if t =:= typeOf[Float] => "Double"
      case t if t =:= typeOf[Double] => "Double"
      case t if t =:= typeOf[String] => "String"
      case t if t =:= typeOf[Boolean] => "Boolean"
      case t if t =:= typeOf[Date] => "Datetime"
      case _ => throw new Exception(s"Unsupported type $myType")
    }

  }

  implicit class PowerBIDStream[A <: Product: TypeTag](stream: DStream[A]) extends PowerBISink {

    override val conf = ClientConf.fromSparkConf(stream.context.sparkContext.getConf)

    /**
     * Inserts data into a PowerBI table. If the dataset does not already exist it will be created
     * along with the specified table and schema based on the incoming data. Optionally clears existing
     * data in the table.
     *
     * @param dataset The dataset name in PowerBI
     * @param table The target table name
     * @param append Whether to append data or clear the table before inserting (default: true)
     * @param batchSize Max number of records to submit in a batch (default: $BATCH_SIZE)
     */
    def saveToPowerBI(dataset: String, table: String, append: Boolean = true, batchSize: Int = ClientConf.BATCH_SIZE): Unit = {
      val ds = Await.result(getOrCreateDataset(dataset, table, typeTag[A]) flatMap { d =>
        append match {
          case true => future(d)
          case false => client.clearTable(d.id, table) map { _ => d}
        }
      }, Duration.Inf) // have to await here otherwise Spark won't see the foreachRDD below

      val clientConf = conf
      val _table = table
      val _batchSize = batchSize

      stream foreachRDD {
        _ foreachPartition { p =>
          val _client = new Client(clientConf)
          val rows = p.toSeq.sliding(_batchSize, _batchSize)
          for (batch <- rows) {
            Await.result(_client.addRows(ds.id, _table, batch), Duration.Inf)
          }
          _client.shutdown()
        }
      }
    }
  }

  implicit class PowerBIRDD[A <: Product : TypeTag](rdd: RDD[A]) extends PowerBISink {

    override val conf = ClientConf.fromSparkConf(rdd.sparkContext.getConf)

    /**
     * Inserts data into a PowerBI table. If the dataset does not already exist it will be created
     * along with the specified table and schema based on the incoming data. Optionally clears existing
     * data in the table.
     *
     * @param dataset The dataset name in PowerBI
     * @param table The target table name
     * @param append Whether to append data or clear the table before inserting (default: true)
     * @param batchSize Max number of records to submit in a batch (default: $BATCH_SIZE)
     */
    def saveToPowerBI(dataset: String, table: String, append: Boolean = true, batchSize: Int = ClientConf.BATCH_SIZE): Unit = {
      val ds = getOrCreateDataset(dataset, table, typeTag[A]) flatMap { d=>
        append match {
          case true => future(d)
          case false => client.clearTable(d.id, table) map { _ => d }
        }
      }

      val result = ds map { d =>
        val clientConf = conf
        val _table = table
        val _batchSize = batchSize

        rdd foreachPartition { p =>
          val _client = new Client(clientConf)
          val rows = p.toSeq.sliding(_batchSize, _batchSize)
          for (batch <- rows) {
            Await.result(_client.addRows(d.id, _table, batch), Duration.Inf)
          }
          _client.shutdown()
        }
      }

      Await.result(result, Duration.Inf)
    }

  }

  implicit class PowerBISchemaRDD(schemaRDD: SchemaRDD) extends PowerBISink {

    override val conf = ClientConf.fromSparkConf(schemaRDD.sparkContext.getConf)

    /**
     * Inserts data into a PowerBI table. If the dataset does not already exist it will be created
     * along with the specified table and schema based on the incoming data. Optionally clears existing
     * data in the table.
     *
     * @param dataset The dataset name in PowerBI
     * @param table The target table name
     * @param append Whether to append data or clear the table before inserting (default: true)
     * @param batchSize Max number of records to submit in a batch (default: $BATCH_SIZE)
     */
    def saveToPowerBI(dataset: String, table: String, append: Boolean = true, batchSize: Int = ClientConf.BATCH_SIZE): Unit = {
      val fields = schemaRDD.schema.fieldNames.zipWithIndex

      val ds = getOrCreateDataset(dataset, table, schemaRDD.schema) flatMap { d =>
        append match {
          case true => future(d)
          case false => client.clearTable(d.id, table) map { _ => d}
        }
      }

      val result = ds map { d =>
        schemaRDD foreachPartition { p =>
          val rows = p.map(r => {
            fields.map{ case(name, index) => (name -> r(index)) }.toMap
          }).toSeq.sliding(batchSize, batchSize)

          val _client = new Client(conf)
          for (batch <- rows) {
            Await.result(_client.addRows(d.id, table, batch), Duration.Inf)
          }
          _client.shutdown()
        }
      }

      Await.result(result, Duration.Inf)
    }

  }

}
