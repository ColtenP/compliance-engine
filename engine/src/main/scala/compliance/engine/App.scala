package compliance.engine

import compliance.engine.models.PolicyType.{Speed, Time => TimePolicyType}
import compliance.engine.models.{Policy, PolicyMatchKey, VehicleEvent, VehiclePolicyMatchUpdate}
import compliance.engine.process.VehicleEventPolicyMatcher
import compliance.engine.sources.FileSourceUtil
import compliance.engine.window.speed.{SpeedViolationProcessWindowFunction, SpeedViolationTrigger}
import compliance.engine.window.time.{TimeViolationProcessWindowFunction, TimeViolationTrigger}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

object App {
   def main(args: Array[String]): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment

      // Create the Sources to be used
//      val policies = PolicyGenerator.create(env).broadcast(VehicleEventPolicyMatcher.POLICY_STATE_DESCRIPTOR)
//      val vehicleEvents = VehicleEventGenerator.create(env)
      val policies = env.fromElements(FileSourceUtil.fromFile[Policy]("policies.json"):_ *).broadcast(VehicleEventPolicyMatcher.POLICY_STATE_DESCRIPTOR)
      val vehicleEvents = env.fromElements(FileSourceUtil.fromFile[VehicleEvent]("events.json"):_ *)

      // Match Policies with Vehicle Events
      val policyEventMatches = vehicleEvents
        .keyBy((in: VehicleEvent) => in.vehicleId)
        .connect(policies)
        .process(new VehicleEventPolicyMatcher)
        .name("event-policy-matcher")

      val speedViolations = policyEventMatches
        .filter(_.policy.policyType == Speed)
        .name("speed-policy-matches")
        .keyBy((policyMatch: VehiclePolicyMatchUpdate) => PolicyMatchKey(policyMatch.policy.id, policyMatch.vehicleEvent.vehicleId))
        .window(EventTimeSessionWindows.withGap(Time.minutes(10)))
        .trigger(new SpeedViolationTrigger)
        .process(new SpeedViolationProcessWindowFunction)
        .name("speed-violation-session-window")

      val timeViolations = policyEventMatches
        .filter(_.policy.policyType == TimePolicyType)
        .name("time-policy-matches")
        .keyBy((policyMatch: VehiclePolicyMatchUpdate) => PolicyMatchKey(policyMatch.policy.id, policyMatch.vehicleEvent.vehicleId))
        .window(EventTimeSessionWindows.withGap(Time.minutes(10)))
        .trigger(new TimeViolationTrigger)
        .process(new TimeViolationProcessWindowFunction)
        .name("time-violation-session-window")

      speedViolations.print("speed-violations")
      timeViolations.print("time-violations")

      env.execute("Vehicle Event Policy Compliance Engine")
   }
}
