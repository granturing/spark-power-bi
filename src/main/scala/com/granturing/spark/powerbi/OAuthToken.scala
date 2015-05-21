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

import java.util.concurrent.{ExecutionException, TimeUnit, Executors}
import com.microsoft.aad.adal4j.AuthenticationContext
import dispatch._
import org.apache.spark.Logging
import scala.util.{Try, Failure, Success}

private class OAuthReq(token: OAuthTokenHandler) extends (Req => Req) {

  override def apply(req: Req): Req = {
    req <:< Map("Authorization" -> s"Bearer ${token()}")
  }

}

private class OAuthTokenHandler(authConf: ClientConf, initialToken: Option[String] = None) extends Logging {

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

  private def refreshToken: Try[String] = {
    log.info("refreshing OAuth token")

    val service = Executors.newFixedThreadPool(1);
    val context = new AuthenticationContext(authConf.token_uri, true, service)

    val future = context.acquireToken(authConf.resource, authConf.clientid, authConf.username, authConf.password, null)

    try {
      val result = future.get(authConf.timeout.toSeconds, TimeUnit.SECONDS)

      log.info("OAuth token refresh successful")

      Success(result.getAccessToken)
    } catch {
      case e: ExecutionException => Failure(e.getCause)
      case t: Throwable => Failure(t)
    } finally {
      service.shutdown()
    }

  }

}

