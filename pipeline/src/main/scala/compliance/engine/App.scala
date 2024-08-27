package compliance.engine

import compliance.engine.models.PolicyType.Speed
import compliance.engine.models.{PolicyMatchKey, VehicleEvent, VehicleEventPolicyMatch}
import compliance.engine.process.VehicleEventPolicyMatcher
import compliance.engine.sources.{PolicyGenerator, VehicleEventGenerator}
import compliance.engine.window.speed.{SpeedViolationProcessWindowFunction, SpeedViolationTrigger}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

object App {
   def main(args: Array[String]): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment

      // Create the Sources to be used
      val policies = PolicyGenerator.create(env).broadcast(VehicleEventPolicyMatcher.POLICY_STATE_DESCRIPTOR)
      val vehicleEvents = VehicleEventGenerator.create(env)

      // Match Policies with Vehicle Events
      val policyEventMatches = vehicleEvents
        .keyBy((in: VehicleEvent) => in.vehicleId)
        .connect(policies)
        .process(new VehicleEventPolicyMatcher)
        .name("event-policy-matcher")

      val speedViolations = policyEventMatches
        .filter(_.policy.policyType == Speed)
        .name("speed-policy-matches")
        .keyBy((policyMatch: VehicleEventPolicyMatch) => PolicyMatchKey(policyMatch.policy.id, policyMatch.vehicleEvent.map(_.vehicleId).get))
        .window(EventTimeSessionWindows.withGap(Time.minutes(10)))
        .trigger(new SpeedViolationTrigger)
        .process(new SpeedViolationProcessWindowFunction)
        .name("speed-violation-session-window")

      speedViolations.print()

      env.execute("Vehicle Event Policy Compliance Engine")
   }
}
