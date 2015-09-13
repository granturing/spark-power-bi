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
import scala.concurrent.duration._

/**
 * Specifies the client configuration options for connecting to the PowerBI service.
 * See [[com.granturing.spark.powerbi.ClientConf.fromSparkConf]] for generating
 * from Spark configuration.
 *
 * @param token_uri the OAuth token URI to authenticate against
 * @param resource the OAuth resource to authenticate for
 * @param uri the PowerBI API URI
 * @param username the Azure Active Directory account name to authenticate with
 * @param password the Azure Active Directory account password to authenticate with
 * @param clientid the OAuth client id
 * @param timeout the response timeout in seconds for API calls
 * @param maxPartitions max number of partitions when saving
 */
case class ClientConf(
  token_uri: String,
  resource: String,
  uri: String,
  username: String,
  password: String,
  clientid: String,
  timeout: Duration,
  maxPartitions: Int,
  batchSize: Int
)

object ClientConf {

  val TOKEN_URI_DEFAULT = "https://login.windows.net/common/oauth2/token"
  val TOKEN_RESOURCE_DEFAULT = "https://analysis.windows.net/powerbi/api"
  val API_URI_DEFAULT = "https://api.powerbi.com/v1.0/myorg"
  val BATCH_SIZE = 10000
  val MAX_PARTITIONS = 5

  private val POWERBI_USERNAME = "POWERBI_USERNAME"
  private val POWERBI_PASSWORD = "POWERBI_PASSWORD"
  private val POWERBI_CLIENTID = "POWERBI_CLIENTID"

  /**
   * Generates a PowerBI client configuration for credentials, URIs, and OAuth client id.
   *
   * Settings are:
   *
   * spark.powerbi.token.uri - The OAuth token URI to authenticate against (default: https://login.windows.net/common/oauth2/token)
   *
   * spark.powerbi.token.resource - The OAuth resource to authenticate for (default: https://analysis.windows.net/powerbi/api)
   *
   * spark.powerbi.uri - The PowerBI API URI (default: https://api.powerbi.com/beta/myorg)
   *
   * spark.powerbi.username - The Azure Active Directory account name to authenticate with
   *
   * spark.powerbi.password - The Azure Active Directory account password to authenticate with
   *
   * spark.pwerbi.clientid - The OAuth client id
   *
   * spark.powerbi.timeout - The response timeout in seconds for API calls (default: 30 seconds)
   *
   * spark.powerbi.max_partitions - The max number of partitions when saving (default: 5)
   *
   * @param conf a Spark configuration object with the application settings
   * @return a PowerBI client configuration
   */
  def fromSparkConf(conf: SparkConf): ClientConf = {
    val token = conf.get("spark.powerbi.token.uri", TOKEN_URI_DEFAULT)
    val resource = conf.get("spark.powerbi.token.resource", TOKEN_RESOURCE_DEFAULT)
    val api = conf.get("spark.powerbi.uri", API_URI_DEFAULT)
    val username = sys.env.getOrElse(POWERBI_USERNAME, conf.get("spark.powerbi.username"))
    val password = sys.env.getOrElse(POWERBI_PASSWORD, conf.get("spark.powerbi.password"))
    val clientid = sys.env.getOrElse(POWERBI_CLIENTID, conf.get("spark.powerbi.clientid"))
    val timeout = Duration(conf.get("spark.powerbi.timeout", "30").toInt, SECONDS)
    val maxPartitions = conf.get("spark.powerbi.max_partitions", MAX_PARTITIONS.toString).toInt
    val batchSize = conf.get("spark.powerbi.batch_size", BATCH_SIZE.toString).toInt

    ClientConf(token, resource, api, username, password, clientid, timeout, maxPartitions, batchSize)
  }
}
