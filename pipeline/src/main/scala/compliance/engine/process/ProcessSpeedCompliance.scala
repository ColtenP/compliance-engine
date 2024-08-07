package compliance.engine.process

import compliance.engine.models.PolicyMatchType.{Matched, Unmatched}
import compliance.engine.models.{PolicyMatchKey, PolicyViolationNotice, PolicyViolationNoticeType, VehicleEventPolicyMatch}
import compliance.engine.process.ProcessSpeedCompliance.VIOLATION_STATE_DESCRIPTOR
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import java.util.UUID

class ProcessSpeedCompliance extends KeyedProcessFunction[PolicyMatchKey, VehicleEventPolicyMatch, PolicyViolationNotice] {
  @transient private var violationState: ValueState[UUID] = _

  override def open(parameters: Configuration): Unit = {
    violationState = getRuntimeContext.getState(VIOLATION_STATE_DESCRIPTOR)
  }

  def processElement(
                      policyMatch: VehicleEventPolicyMatch,
                      ctx: KeyedProcessFunction[PolicyMatchKey, VehicleEventPolicyMatch, PolicyViolationNotice]#Context,
                      out: Collector[PolicyViolationNotice]
                    ): Unit = {
    policyMatch.matchType match {
      case Matched =>
        val existingViolatedRuleIdOption = Option(violationState.value())
        val violatedRuleIdOption = policyMatch.policy.rules.find { rule =>
          rule.maximum.exists(_ >= policyMatch.vehicleEvent.map(_.speed).getOrElse(Double.MinValue)) ||
          rule.minimum.exists(_ <= policyMatch.vehicleEvent.map(_.speed).getOrElse(Double.MaxValue))
        }.map(_.id)

        if (violatedRuleIdOption.nonEmpty && existingViolatedRuleIdOption.nonEmpty && violatedRuleIdOption != existingViolatedRuleIdOption) {
          out.collect(
            PolicyViolationNotice(
              policyId = policyMatch.policy.id,
              ruleId = existingViolatedRuleIdOption.get,
              vehicleId = policyMatch.vehicleEvent.map(_.vehicleId),
              slot = None,
              noticeType = PolicyViolationNoticeType.Ended,
              timestamp = policyMatch.vehicleEvent.map(_.timestamp).getOrElse(ctx.timerService().currentWatermark())
            )
          )

          out.collect(
            PolicyViolationNotice(
              policyId = policyMatch.policy.id,
              ruleId = violatedRuleIdOption.get,
              vehicleId = policyMatch.vehicleEvent.map(_.vehicleId),
              slot = None,
              noticeType = PolicyViolationNoticeType.Started,
              timestamp = policyMatch.vehicleEvent.map(_.timestamp).getOrElse(ctx.timerService().currentWatermark())
            )
          )
        } else if (violatedRuleIdOption.nonEmpty && existingViolatedRuleIdOption.isEmpty) {
          out.collect(
            PolicyViolationNotice(
              policyId = policyMatch.policy.id,
              ruleId = violatedRuleIdOption.get,
              vehicleId = policyMatch.vehicleEvent.map(_.vehicleId),
              slot = None,
              noticeType = PolicyViolationNoticeType.Started,
              timestamp = policyMatch.vehicleEvent.map(_.timestamp).getOrElse(ctx.timerService().currentWatermark())
            )
          )
        } else if (violatedRuleIdOption.isEmpty && existingViolatedRuleIdOption.nonEmpty) {
          out.collect(
            PolicyViolationNotice(
              policyId = policyMatch.policy.id,
              ruleId = existingViolatedRuleIdOption.get,
              vehicleId = policyMatch.vehicleEvent.map(_.vehicleId),
              slot = None,
              noticeType = PolicyViolationNoticeType.Ended,
              timestamp = policyMatch.vehicleEvent.map(_.timestamp).getOrElse(ctx.timerService().currentWatermark())
            )
          )
        }

        if (violatedRuleIdOption.nonEmpty) {
          violatedRuleIdOption.foreach(violationState.update)
        } else {
          violationState.clear()
        }

      case Unmatched =>
        Option(violationState.value()).foreach { violatedRuleId =>
          out.collect(
            PolicyViolationNotice(
              policyId = ctx.getCurrentKey.policyId,
              ruleId = violatedRuleId,
              vehicleId = policyMatch.vehicleEvent.map(_.vehicleId),
              slot = None,
              noticeType = PolicyViolationNoticeType.Ended,
              timestamp = ctx.timerService().currentWatermark()
            )
          )
        }
    }
  }
}

object ProcessSpeedCompliance {
  val VIOLATION_STATE_DESCRIPTOR = new ValueStateDescriptor[UUID]("ViolationState", TypeInformation.of(classOf[UUID]))
}