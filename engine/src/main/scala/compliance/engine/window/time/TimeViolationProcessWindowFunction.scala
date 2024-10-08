package compliance.engine.window.time

import compliance.engine.models.{PolicyMatchKey, PolicyViolation, TimePolicyMatchUpdate}
import compliance.engine.traits.Loggable
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.lang
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class TimeViolationProcessWindowFunction
  extends ProcessWindowFunction[TimePolicyMatchUpdate, PolicyViolation, PolicyMatchKey, TimeWindow]
    with Loggable {
  def process(
               key: PolicyMatchKey,
               context: ProcessWindowFunction[TimePolicyMatchUpdate, PolicyViolation, PolicyMatchKey, TimeWindow]#Context,
               elements: lang.Iterable[TimePolicyMatchUpdate],
               out: Collector[PolicyViolation]
             ): Unit = {
    val matches = elements.asScala.toSeq.sortBy(_.eventTimestamp)

    // If there are no matches, then you cannot create a violation
    if (matches.isEmpty) return

    val startOption = matches
      .headOption
      .map(_.eventTimestamp)
    val endOption = matches
      .lastOption
      .map(_.eventTimestamp)

    if (startOption.isDefined && endOption.isDefined) {
      val start = startOption.get
      val end = endOption.get

      if (start <= end) {
        val duration = end - start

        // If no rule was violated, then there's no violation
        val didViolationOccur = matches.head.ruleMaximum.exists(_ <= duration) ||
          matches.head.ruleMaximum.exists(_ >= duration)

        if (didViolationOccur) {
          out.collect(PolicyViolation(
            policyId = key.policyId,
            vehicleId = key.vehicleId,
            start = context.window().getStart,
            end = context.window().getEnd
          ))
        }
      } else {
        LOGGER.warn(s"The start violation is not less than the end violation, $start >= $end")
      }
    } else {
      LOGGER.error(s"A session window was created with no violating events in it: ${key.policyId} - ${key.vehicleId}")
    }
  }
}
