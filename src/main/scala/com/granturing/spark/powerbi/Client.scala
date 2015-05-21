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

import java.net.URLEncoder
import java.text.SimpleDateFormat
import java.util.concurrent.Executors
import com.ning.http.client.{AsyncHttpClient, AsyncHttpClientConfig, Response}
import dispatch._, Defaults._
import org.apache.spark.Logging
import org.json4s.JsonAST.JNull
import org.json4s.jackson.JsonMethods._
import org.json4s.{JValue, DefaultFormats}
import org.json4s.jackson.Serialization._
import scala.concurrent.Future

case class Dataset(id: String, name: String)

case class Column(name: String, dataType: String)

case class Table(name: String, columns: Seq[Column])

case class Schema(name: String, tables: Seq[Table])

private[powerbi] object PowerBIResult extends (Response => JValue) {

  implicit private val formats = DefaultFormats

  override def apply(response: Response): JValue = response.getStatusCode match {
    case x if (200 until 300 contains x) && response.hasResponseBody => parse(response.getResponseBody)
    case x if 200 until 300 contains x => JNull
    case _ if s"${response.getContentType}".startsWith("application/json") && response.hasResponseBody => {
      val json = parse(response.getResponseBody)
      val error = (json \ "message").extract[String]
      val details = ((json \ "details") \ "message").extractOrElse("")

      throw new Exception(s"$error: $details")
    }
    case _ if response.getResponseBody.size == 0 => throw new Exception(response.getStatusText)
    case _ => throw new Exception(response.getResponseBody)
  }
}

/**
 * A very basic PowerBI client using the Scala Dispatch HTTP library. Requires that an app be registered
 * in your Azure Active Directory to allow access to your PowerBI service.
 *
 * @param conf a client configuration
 * @see [[com.granturing.spark.powerbi.ClientConf]]
 */
class Client(conf: ClientConf, initialToken: Option[String] = None) extends Logging {

  implicit private val formats = new DefaultFormats {
    override def dateFormatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS")
  }

  private val threadPool = Executors.newCachedThreadPool()

  private val httpConfig = new AsyncHttpClientConfig.Builder()
    .setExecutorService(threadPool)
    .setConnectionTimeoutInMs(conf.timeout.toMillis.toInt)
    .setRequestTimeoutInMs(conf.timeout.toMillis.toInt)
    .setCompressionEnabled(true)
    .build()

  @transient lazy private val http = new Http(new AsyncHttpClient(httpConfig))

  private val token = new OAuthTokenHandler(conf, initialToken)

  private val oauth = new OAuthReq(token)

  /**
   * Gets the current OAuth token being used for authentication.
   *
   * @return an OAuth authorization token
   */
  def currentToken = token()

  /**
   * Gets a list of datasets for the current account.
   *
   * @return a list of datasets
   * @see [[com.granturing.spark.powerbi.Dataset]]
   */
  def getDatasets: Future[List[Dataset]] = {
    val datasets_req = url(conf.uri + "/datasets")

    val request = http(oauth(datasets_req) > PowerBIResult)

    val response = request map { json => (json \ "datasets").extract[List[Dataset]] }

    response
  }

  /**
   * Creates a new dataset with the specified schema
   *
   * @param schema a schema for the new dataset
   * @return a dataset object for the newly created dataset
   * @see [[com.granturing.spark.powerbi.Schema]]
   * @see [[com.granturing.spark.powerbi.Dataset]]
   */
  def createDataset(schema: Schema): Future[Dataset] = {
    val body = write(schema)

    val create_req = url(conf.uri + "/datasets")
      .POST
      .setContentType("application/json", "UTF-8") <<
      body

    val request = http(oauth(create_req) > PowerBIResult)

    val response = request map { json => json.extract[Dataset] }

    response
  }

  /**
   * Gets a list of tables for the specified dataset.
   *
   * @param dataset a dataset GUID
   * @return a list of tables
   */
  def getTables(dataset: String): Future[Seq[String]] = {
    val tables_req = url(conf.uri + "/datasets/" + dataset + "/tables")

    val request = http(oauth(tables_req) > PowerBIResult)

    val response = request map { json => (json \ "tables" \ "name").extract[Seq[String]] }

    response
  }

  /**
   * Updates the schema of an existing table
   *
   * @param dataset a dataset GUID
   * @param table the table name which to update
   * @return a success or failure result
   */
  def updateTableSchema(dataset: String, table: String, schema: Table): Future[Unit] = {
    val body = write(schema)

    val add_req = url(conf.uri + "/datasets/" + dataset + "/tables/" + URLEncoder.encode(table, "UTF-8"))
      .PUT
      .setContentType("application/json", "UTF-8") <<
      body

    val request = http(oauth(add_req) > PowerBIResult)

    request.map(_ => ())
  }

  /**
   * Adds a collection of rows to the specified dataset and table. If the dataset or table
   * do not exist an error will be returned.
   *
   * @param dataset a dataset GUID
   * @param table a table name within the dataset
   * @param rows a sequence of JSON serializable objects with property names matching the schema
   * @return a success or failure result
   */
  def addRows(dataset: String, table: String, rows: Seq[_]): Future[Unit] = {
    val body = write("rows" -> rows)

    val add_req = url(conf.uri + "/datasets/" + dataset + "/tables/" + URLEncoder.encode(table, "UTF-8") + "/rows")
      .POST
      .setContentType("application/json", "UTF-8") <<
      body

    val request = http(oauth(add_req) > PowerBIResult)

    request.map(_ => ())
  }

  /**
   * Clears all rows in the specified table.
   *
   * @param dataset a dataset GUID
   * @param table a table name within the dataset
   * @return a success or failure result
   */
  def clearTable(dataset: String, table: String): Future[Unit] = {
    val add_req = url(conf.uri + "/datasets/" + dataset + "/tables/" + URLEncoder.encode(table, "UTF-8") + "/rows")
      .DELETE

    val request = http(oauth(add_req) > PowerBIResult)

    request.map(_ => ())
  }

  def shutdown(): Unit = {
    http.shutdown()
    threadPool.shutdown()
  }
}
