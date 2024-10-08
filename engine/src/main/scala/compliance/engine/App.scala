package compliance.engine

import compliance.engine.models._
import compliance.engine.process.VehicleEventPolicyMatcher
import compliance.engine.sources.FileSourceUtil
import compliance.engine.window.speed.{SpeedViolationProcessWindowFunction, SpeedViolationReducer, SpeedViolationTrigger}
import compliance.engine.window.time.{TimeViolationProcessWindowFunction, TimeViolationTrigger}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration

object App {
   def main(args: Array[String]): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment

      // Create the Sources to be used
//      val policies = PolicyGenerator.create(env).broadcast(VehicleEventPolicyMatcher.POLICY_STATE_DESCRIPTOR)
//      val vehicleEvents = VehicleEventGenerator.create(env)
      val policies = env.fromElements(FileSourceUtil.fromFile[Policy]("policies.json"):_ *)
        .assignTimestampsAndWatermarks(
           WatermarkStrategy
             .forBoundedOutOfOrderness(Duration.ZERO)
             .withTimestampAssigner(new SerializableTimestampAssigner[Policy] {
                def extractTimestamp(t: Policy, l: Long): Long = Long.MaxValue
             })
        )
        .broadcast(VehicleEventPolicyMatcher.POLICY_STATE_DESCRIPTOR)
      val vehicleEvents = env.fromElements(FileSourceUtil.fromFile[VehicleEvent]("events.json"):_ *)
        .assignTimestampsAndWatermarks(
           WatermarkStrategy
             .forMonotonousTimestamps()
             .withTimestampAssigner(new SerializableTimestampAssigner[VehicleEvent] {
                def extractTimestamp(t: VehicleEvent, l: Long): Long = t.timestamp
             })
        )

      // Match Policies with Vehicle Events
      val policyEventMatches = vehicleEvents
        .keyBy((in: VehicleEvent) => in.vehicleId)
        .connect(policies)
        .process(new VehicleEventPolicyMatcher)
        .name("event-policy-matcher")

      val speedViolations = policyEventMatches
        .getSideOutput(VehicleEventPolicyMatcher.SPEED_POLICY_MATCHES)
        .keyBy((policyMatch: SpeedPolicyMatchUpdate) => PolicyMatchKey(policyMatch.policyId, policyMatch.vehicleId))
        .window(EventTimeSessionWindows.withGap(Time.minutes(10)))
        .trigger(new SpeedViolationTrigger)
        .reduce(new SpeedViolationReducer, new SpeedViolationProcessWindowFunction)
        .name("speed-violation-session-window")

      val timeViolations = policyEventMatches
        .getSideOutput(VehicleEventPolicyMatcher.TIME_POLICY_MATCHES)
        .keyBy((policyMatch: TimePolicyMatchUpdate) => PolicyMatchKey(policyMatch.policyId, policyMatch.vehicleId))
        .window(EventTimeSessionWindows.withGap(Time.minutes(10)))
        .trigger(new TimeViolationTrigger)
        .process(new TimeViolationProcessWindowFunction)
        .name("time-violation-session-window")

      speedViolations.print("speed-violations")
      timeViolations.print("time-violations")

      env.execute("Vehicle Event Policy Compliance Engine")
   }
}
