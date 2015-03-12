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

import dispatch._, Defaults._
import org.apache.spark.Logging
import org.json4s._
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import scala.concurrent.Await
import scala.util.{Failure, Success}

private class OAuthReq(token: OAuthTokenHandler) extends (Req => Req) {

  override def apply(req: Req): Req = {
    req <:< Map("Authorization" -> s"Bearer ${token()}")
  }

}

private class OAuthTokenHandler(authConf: ClientConf, initialToken: Option[String] = None)(implicit http: Http) extends Logging {
  implicit private val formats = DefaultFormats

  private var _token: Option[String] = initialToken

  def apply(refresh: Boolean = false): String = {
    _token match {
      case Some(s) if !refresh => s
      case _ => {
        refreshToken match {
          case Success(s) => {
            _token = Some(s)
            s
          }
          case Failure(e) => throw e
        }
      }
    }
  }

  private def refreshToken = {
    log.info("refreshing OAuth token")

    val token_req = url(authConf.token_uri) <<
      Map("grant_type" -> "password",
        "username" -> authConf.username,
        "password" -> authConf.password,
        "client_id" -> authConf.clientid,
        "resource" -> authConf.resource)

    val request = http(token_req)

    val response = Await.result(request, authConf.timeout)

    response.getStatusCode match {
      case 200 => {
        val token = (parse(response.getResponseBody) \ "access_token").extract[String]

        log.info("token refresh successful")

        Success(token)
      }
      case _ if s"${response.getContentType}".startsWith("application/json") && response.getResponseBody.size > 0 => {
        val json = parse(response.getResponseBody)
        val error = (json \ "error_description").extract[String]

        Failure(new Exception(error))
      }
      case _ if response.getResponseBody.size == 0 => Failure(new Exception(response.getStatusText))
      case _ => Failure(new Exception(response.getResponseBody))
    }
  }

}

