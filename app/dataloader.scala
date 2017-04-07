import java.io.File
import javax.inject._

import com.google.inject.ImplementedBy
import play.api.Logger

@ImplementedBy(classOf[ElasticsearchDataLoader])
trait DataLoader {
  def load(): Unit
}

class ElasticsearchDataLoader @Inject()(environment: play.api.Environment) extends DataLoader {

  val logger = Logger("ElasticsearchDataLoader")

  private def allFiles(dir: File, extensions: Seq[String]): Seq[File] = {
    val files: Seq[File] = dir.listFiles
    val matchingFiles = files.filter { file =>
      file.isFile && extensions.exists(file.getName.endsWith(_))
    }
    matchingFiles ++ files.filter(_.isDirectory).flatMap(allFiles(_, extensions))
  }

  def load(): Unit = {
    // Conferences
    val conferencesDir = environment.getFile("/conf/data/conferences")
    logger.info(s"Loading conferences into elasticsearch from directory $conferencesDir")
    allFiles(conferencesDir, Seq("json")).foreach(f => logger.info(s"Loading ${f.getName}"))

    // Meetups
    val meetupsDir = environment.getFile("/conf/data/meetups")
    logger.info(s"Loading meetups into elasticsearch from directory $meetupsDir")
    allFiles(meetupsDir, Seq("json")).foreach(f => logger.info(s"Loading ${f.getName}"))
  }

  load()
}