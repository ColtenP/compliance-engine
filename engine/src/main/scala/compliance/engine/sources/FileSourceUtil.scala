package compliance.engine.sources

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.io.Source

object FileSourceUtil {
  private lazy val objectMapper = new ObjectMapper()
    .registerModule(new DefaultScalaModule())

  def fromFile[T](filePath: String)(implicit manifest: Manifest[T]): List[T] = {
    val rawJson = Source.fromResource(filePath).getLines().mkString("\n")
    objectMapper.readValue(rawJson, objectMapper.getTypeFactory.constructArrayType(manifest.runtimeClass))
      .asInstanceOf[Array[T]]
      .toList
  }
}
