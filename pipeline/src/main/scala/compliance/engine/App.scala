package compliance.engine

import compliance.engine.models.PolicyType.Speed
import compliance.engine.models.{VehicleEvent, VehicleEventPolicyMatch}
import compliance.engine.process.{ProcessSpeedCompliance, VehicleEventPolicyMatcher}
import compliance.engine.sources.{PolicyGenerator, VehicleEventGenerator}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

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


      val speedViolations = policyEventMatches
        .filter(_.policy.policyType == Speed)
        .keyBy((policyMatch: VehicleEventPolicyMatch) => (policyMatch.policy.id, policyMatch.vehicleEvent.map(_.vehicleId).get))
        .process(new ProcessSpeedCompliance)

      speedViolations.print()

      env.execute("Vehicle Event Policy Compliance Engine")
   }
}
