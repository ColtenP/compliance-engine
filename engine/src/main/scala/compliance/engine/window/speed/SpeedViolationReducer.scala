package compliance.engine.window.speed

import compliance.engine.models.{PolicyMatchKey, PolicyViolation, SpeedPolicyMatchUpdate}
import compliance.engine.traits.Loggable
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.lang
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class SpeedViolationReducer
  extends ReduceFunction[SpeedPolicyMatchUpdate]
    with Loggable {
  def reduce(agg: SpeedPolicyMatchUpdate, curr: SpeedPolicyMatchUpdate): SpeedPolicyMatchUpdate = {
    if (agg.eventTimestamp < curr.eventTimestamp) {
      agg
    } else {
      curr
    }
  }
}
