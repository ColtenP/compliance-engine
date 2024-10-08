package compliance.engine.window.speed

import compliance.engine.models.{PolicyMatchKey, PolicyViolation, SpeedPolicyMatchUpdate}
import compliance.engine.traits.Loggable
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.lang
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class SpeedViolationProcessWindowFunction
  extends ProcessWindowFunction[SpeedPolicyMatchUpdate, PolicyViolation, PolicyMatchKey, TimeWindow]
    with Loggable {
  def process(
               key: PolicyMatchKey,
               context: ProcessWindowFunction[SpeedPolicyMatchUpdate, PolicyViolation, PolicyMatchKey, TimeWindow]#Context,
               elements: lang.Iterable[SpeedPolicyMatchUpdate],
               out: Collector[PolicyViolation]
             ): Unit = {
    val matches = elements.asScala.toSeq.sortBy(_.eventTimestamp)

    // If there are no matches, then you cannot create a violation
    if (matches.isEmpty) return

    val startOption = matches.headOption.map(_.eventTimestamp)

    if (startOption.isDefined) {
      val start = startOption.get
      val end = context.window().getEnd

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
