package compliance.engine.sources

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.streaming.api.functions.source.SourceFunction

import java.util.concurrent.atomic.AtomicBoolean

class ArraySource[OUT](data: List[OUT], clazzOUT: Class[OUT]) extends SourceFunction[OUT] with ResultTypeQueryable[OUT] {
  private val running = new AtomicBoolean(true)
  private var index: Int = 0

  def run(sourceContext: SourceFunction.SourceContext[OUT]): Unit = {
    while (running.get()) {
      if (index < data.length) {
        val record = data(index)
        sourceContext.collect(record)
        index += 1
      }
    }
  }

  def cancel(): Unit = {
    running.set(false)
  }

  def getProducedType: TypeInformation[OUT] = TypeInformation.of(clazzOUT)
}
