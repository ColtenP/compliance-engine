package compliance.engine.sinks

import org.apache.flink.api.connector.sink2.{Sink, SinkWriter}
import org.apache.flink.streaming.api.functions.sink.SinkFunction

class DummySink[T] extends Sink[T] {
  def createWriter(initContext: Sink.InitContext): SinkWriter[T] = new DummySinkWriter()
}

class DummySinkWriter[T] extends SinkWriter[T] {
  def write(inputT: T, context: SinkWriter.Context): Unit = {}

  def flush(b: Boolean): Unit = {}

  def close(): Unit = {}
}
