import java.io.{File, FileInputStream}
import javax.inject._

import com.google.inject.ImplementedBy
import play.api.libs.json.{JsArray, JsObject, Json}
import play.api.{Environment, Logger}
import utils.ElasticsearchClient

import scala.concurrent.{ExecutionContext, Future}

@ImplementedBy(classOf[ElasticsearchDataLoader])
trait DataLoader {
  def load(): Unit
}

class ElasticsearchDataLoader @Inject()(esClient: ElasticsearchClient, environment: Environment)(implicit ec: ExecutionContext) extends DataLoader {

  val logger = Logger("ElasticsearchDataLoader")

  private def allFiles(dir: File, extensions: Seq[String]): Seq[File] = {
    val files: Seq[File] = dir.listFiles
    val matchingFiles = files.filter { file =>
      file.isFile && extensions.exists(file.getName.endsWith(_))
    }
    matchingFiles ++ files.filter(_.isDirectory).flatMap(allFiles(_, extensions))
  }

  def bulk(baseDir: File, index: String, `type`: String): Future[Either[String, Int]] = {
    logger.info(s"Loading $index into elasticsearch from directory $baseDir")

    val bulkBody: String = allFiles(baseDir, Seq("json")).flatMap(f => {
      logger.info(s"Adding ${f.getName} to bulk body")
      val jsonDocument: JsObject = Json.parse(new FileInputStream(f)).as[JsObject]
      val id: String = (jsonDocument \ "id").as[String]
      Seq(
        s"""{ \"index\" : { \"_index\" : \"$index\", \"_type\" : \"${`type`}\", \"_id\": \"$id\" } }""",
        (jsonDocument - "id").toString()
      )
    }).mkString("\n") + "\n"

    esClient.bulk(bulkBody).map { response =>
      (response.json \ "error").asOpt[JsObject] match {
        case None =>
          val items = (response.json \ "items").as[JsArray].value
          Right(items.length)
        case Some(error) =>
          Left((error \ "type").as[String] + " - " + (error \ "reason").as[String])
      }
    }
  }

  // TODO use aliases
  def load(): Unit = {
    // Conferences
    val conferencesDir: File = environment.getFile("/conf/data/conferences")
    bulk(conferencesDir, "conferences", "conference").map {
      case Left(error) => logger.error(error)
      case Right(nbDocs) => logger.info(s"$nbDocs conferences inserted!")
    }
    // Meetups
    val meetupsDir: File = environment.getFile("/conf/data/meetups")
    bulk(meetupsDir, "meetups", "meetup").map {
      case Left(error) => logger.error(error)
      case Right(nbDocs) => logger.info(s"$nbDocs meetups inserted!")
    }
  }

  load()
}