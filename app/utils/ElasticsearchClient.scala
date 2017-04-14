package utils

import javax.inject.{Inject, Singleton}

import play.api.libs.ws.{WSAuthScheme, WSClient, WSRequest, WSResponse}
import play.api.{Configuration, Environment, Logger}

import scala.concurrent.{ExecutionContext, Future}

@Singleton
class ElasticsearchClient @Inject()(ws: WSClient, environment: Environment, configuration: Configuration)(implicit ec: ExecutionContext) {

  val logger = Logger("ElasticsearchClient")

  val elasticsearchUri: String = configuration.getString("elasticsearch.uri").getOrElse("http://localhost:9200")
  val elasticsearchUser: Option[String] = configuration.getString("elasticsearch.user")
  val elasticsearchPassword: Option[String] = configuration.getString("elasticsearch.password")
  logger.info(s"Elasticsearch URI = $elasticsearchUri")

  private def client(path: String): WSRequest = {
    val sanitizedPath: String = if (path.startsWith("/")) {
      path
    } else {
      s"/$path"
    }

    elasticsearchUser match {
      case None => ws.url(s"$elasticsearchUri$sanitizedPath")
      case Some(user) => ws.url(s"$elasticsearchUri$sanitizedPath").withAuth(user, elasticsearchPassword.getOrElse(""), WSAuthScheme.BASIC)
    }

  }

  def bulk(body: String): Future[WSResponse] = {
    client(s"/_bulk").post(body)
  }

}
