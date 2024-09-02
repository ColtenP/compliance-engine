package compliance.engine.process

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector

class Jsonifier[T<: Any] extends ProcessFunction[T, String] {
  @transient private lazy val objectMapper = new ObjectMapper()
    .registerModule(new DefaultScalaModule())

  def processElement(record: T, context: ProcessFunction[T, String]#Context, collector: Collector[String]): Unit =
    collector.collect(objectMapper.writeValueAsString(record))
}
