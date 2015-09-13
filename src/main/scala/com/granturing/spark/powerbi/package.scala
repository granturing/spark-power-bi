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
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.streaming.dstream.DStream
import scala.concurrent.{Future, Await, future}
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration._
import scala.reflect.runtime.universe._

package object powerbi {

  private[powerbi] trait PowerBISink extends Serializable {

    protected def getGroupId(group: Option[String])(implicit client: Client): Future[Option[String]] = group match {
      case Some(grp) => {
        client.getGroups map { list =>
          val grpOpt = list.filter(g => grp.equals(g.name)).map(_.id).headOption

          grpOpt match {
            case Some(g) => Some(g)
            case None => sys.error(s"group $grp not found")
          }
        }
      }
      case None => future(None)
    }

    protected def getOrCreateDataset[A <: Product: TypeTag](
        groupId: Option[String],
        dataset: String,
        table: String,
        tag: TypeTag[A])(implicit client: Client): Future[Dataset] = {

      client.getDatasets(groupId) flatMap { list =>
        val dsOpt = list.filter(d => dataset.equals(d.name)).headOption

        dsOpt match {
          case Some(d) => future(d)
          case None => client.createDataset(Schema(dataset, Seq(Table(table, schemaFor))), groupId, RetentionPolicy.BasicFIFO)
        }
      }

    }

    protected def getOrCreateDataset(
        mode: SaveMode,
        groupId: Option[String],
        dataset: String,
        table: String,
        schema: StructType)(implicit client: Client): Future[Dataset] = {

      client.getDatasets(groupId) flatMap { list =>
        val dsOpt = list.filter(d => dataset.equals(d.name)).headOption

        (dsOpt, mode) match {
          case (Some(d), SaveMode.ErrorIfExists) => sys.error(s"table $table already exists")
          case (Some(d), SaveMode.Overwrite) => client.clearTable(d.id, table, groupId) map { _ => d }
          case (Some(d), _) => future(d)
          case (None, _) => client.createDataset(Schema(dataset, Seq(Table(table, schemaFor(schema)))), groupId, RetentionPolicy.BasicFIFO)
        }
      }

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
      case ByteType | ShortType | IntegerType | LongType => "Int64"
      case FloatType | DoubleType => "Double"
      case _: DecimalType => "Double"
      case StringType => "String"
      case BooleanType => "Boolean"
      case TimestampType | DateType => "Datetime"
      case _ => throw new Exception(s"Unsupported type $myType")
    }

    // scalastyle:off cyclomatic.complexity
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

    private val conf = ClientConf.fromSparkConf(stream.context.sparkContext.getConf)

    private implicit val client = new Client(conf)

    /**
     * Inserts data into a PowerBI table. If the dataset does not already exist it will be created
     * along with the specified table and schema based on the incoming data. Optionally clears existing
     * data in the table.
     *
     * @param dataset The dataset name in PowerBI
     * @param table The target table name
     * @param append Whether to append data or clear the table before inserting (default: true)
     * @param group Power BI group to use when performing operations (default: None)
     */
    def saveToPowerBI(dataset: String, table: String, append: Boolean = true, group: Option[String] = None): Unit = {

      val step = for {
        groupId <- getGroupId(group)
        ds <- getOrCreateDataset(groupId, dataset, table, typeTag[A]) flatMap { d =>
          append match {
            case true => future(d)
            case false => client.clearTable(d.id, table, groupId) map { _ => d}
          }
        }
      } yield (groupId, ds)

      val (groupId, ds) = Await.result(step, conf.timeout) // have to await here otherwise Spark won't see the foreachRDD below

      // we need local copies, workaround for TypeTag serialization issue (see: https://issues.scala-lang.org/browse/SI-5919)
      val _conf = conf
      val _token = Some(client.currentToken)
      val _table = table

      stream foreachRDD { rdd =>

        val coalesced = rdd.partitions.size > _conf.maxPartitions match {
          case true => rdd.coalesce(_conf.maxPartitions)
          case false => rdd
        }

        coalesced foreachPartition { p =>
          val _client = new Client(_conf, _token)
          val rows = p.toSeq.sliding(_conf.batchSize, _conf.batchSize)

          val submit = rows.
            foldLeft(future()) { (fAccum, batch) =>
            fAccum flatMap { _ => _client.addRows(ds.id, _table, batch, groupId) } }

          submit.onComplete { _ => _client.shutdown() }

          Await.result(submit, _conf.timeout)
        }
      }
    }
  }

  implicit class PowerBIRDD[A <: Product : TypeTag](rdd: RDD[A]) extends PowerBISink {

    private val conf = ClientConf.fromSparkConf(rdd.sparkContext.getConf)

    private implicit val client = new Client(conf)

    /**
     * Inserts data into a PowerBI table. If the dataset does not already exist it will be created
     * along with the specified table and schema based on the incoming data. Optionally clears existing
     * data in the table.
     *
     * @param dataset The dataset name in PowerBI
     * @param table The target table name
     * @param append Whether to append data or clear the table before inserting (default: true)
     * @param group Power BI group to use when performing operations (default: None)
     */
    def saveToPowerBI(dataset: String, table: String, append: Boolean = true, group: Option[String] = None): Unit = {

      val step = for {
        groupId <- getGroupId(group)
        ds <- getOrCreateDataset(groupId, dataset, table, typeTag[A]) flatMap { d =>
          append match {
            case true => future(d)
            case false => client.clearTable(d.id, table, groupId) map { _ => d}
          }
        }
      } yield (groupId, ds)

      val result = step map { case (groupId, ds) =>
        // we need local copies, workaround for TypeTag serialization issue (see: https://issues.scala-lang.org/browse/SI-5919)
        val _conf = conf
        val _token = Some(client.currentToken)
        val _table = table

        val coalesced = rdd.partitions.size > _conf.maxPartitions match {
          case true => rdd.coalesce(_conf.maxPartitions)
          case false => rdd
        }

        coalesced foreachPartition { p =>
          val _client = new Client(_conf, _token)
          val rows = p.toSeq.sliding(_conf.batchSize, _conf.batchSize)

          val submit = rows.
            foldLeft(future()) { (fAccum, batch) =>
              fAccum flatMap { _ => _client.addRows(ds.id, _table, batch, groupId) }
            }

          submit.onComplete { _ => _client.shutdown() }

          Await.result(submit, _conf.timeout)
        }
      }

      Await.result(result, Duration.Inf)
    }

  }

}
