package compliance.engine.process

import compliance.engine.models.UserEntryEvent
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter

class SteveCounterMetric extends RichMapFunction[UserEntryEvent, UserEntryEvent] {
  @transient private var steveCounter: Counter = _

  override def open(parameters: Configuration): Unit = {
    steveCounter = getRuntimeContext
      .getMetricGroup
      .counter("steve")
  }

  def map(in: UserEntryEvent): UserEntryEvent = {
    if (in.name.equalsIgnoreCase("Steve")) {
      steveCounter.inc()
    }

    in
  }
}
