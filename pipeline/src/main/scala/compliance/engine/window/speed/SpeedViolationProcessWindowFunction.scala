package compliance.engine.window.speed

import compliance.engine.models.{PolicyMatchKey, PolicyViolation, VehicleEventPolicyMatch}
import compliance.engine.traits.Loggable
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.lang
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class SpeedViolationProcessWindowFunction
  extends ProcessWindowFunction[VehicleEventPolicyMatch, PolicyViolation, PolicyMatchKey, TimeWindow]
    with Loggable {
  def process(
               key: PolicyMatchKey,
               context: ProcessWindowFunction[VehicleEventPolicyMatch, PolicyViolation, PolicyMatchKey, TimeWindow]#Context,
               elements: lang.Iterable[VehicleEventPolicyMatch],
               out: Collector[PolicyViolation]
             ): Unit = {
    val matches = elements.asScala.toSeq
    val startOption = matches
      .find(m => m.policy.findFirstRuleInViolation(m.vehicleEvent).isDefined)
      .flatMap(_.vehicleEvent)
      .map(_.timestamp)
    val endOption = matches
      .reverse
      .find(m => m.policy.findFirstRuleInViolation(m.vehicleEvent).isDefined)
      .flatMap(_.vehicleEvent)
      .map(_.timestamp)

    if (startOption.isDefined && endOption.isDefined) {
      val start = startOption.get
      val end = endOption.get

      if (start < end) {
        out.collect(PolicyViolation(
          policyId = key.policyId, vehicleId = key.vehicleId, start = start, end = end
        ))
      } else {
        LOGGER.warn(s"The start violation is not less than the end violation, $start >= $end")
      }
    } else {
      LOGGER.error(s"A session window was created with no violating events in it: ${key.policyId} - ${key.vehicleId}")
    }
  }
}
