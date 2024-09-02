package compliance.engine.window.speed

import compliance.engine.models.{PolicyMatchKey, PolicyViolation, VehiclePolicyMatchUpdate}
import compliance.engine.traits.Loggable
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.lang
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class SpeedViolationProcessWindowFunction
  extends ProcessWindowFunction[VehiclePolicyMatchUpdate, PolicyViolation, PolicyMatchKey, TimeWindow]
    with Loggable {
  def process(
               key: PolicyMatchKey,
               context: ProcessWindowFunction[VehiclePolicyMatchUpdate, PolicyViolation, PolicyMatchKey, TimeWindow]#Context,
               elements: lang.Iterable[VehiclePolicyMatchUpdate],
               out: Collector[PolicyViolation]
             ): Unit = {
    val matches = elements.asScala.toSeq.sortBy(_.vehicleEvent.timestamp)

    // If there are no matches, then you cannot create a violation
    if (matches.isEmpty) return

    val startOption = matches
      .find(m => m.policy.findFirstRuleInViolation(m.vehicleEvent).isDefined)
      .map(_.vehicleEvent.timestamp)
    val endOption = matches
      .reverse
      .find(m => m.policy.findFirstRuleInViolation(m.vehicleEvent).isDefined)
      .map(_.vehicleEvent.timestamp)

    if (startOption.isDefined && endOption.isDefined) {
      val start = startOption.get
      val end = endOption.get

      if (start <= end) {
        out.collect(PolicyViolation(
          policyId = key.policyId,
          vehicleId = key.vehicleId,
          start = context.window().getStart,
          end = context.window().getEnd
        ))
      } else {
        LOGGER.warn(s"The start violation is not less than the end violation, $start >= $end")
      }
    } else {
      LOGGER.error(s"A session window was created with no violating events in it: ${key.policyId} - ${key.vehicleId}")
    }
  }
}
