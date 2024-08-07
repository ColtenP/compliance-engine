package compliance.engine.process

import compliance.engine.models.PolicyMatchType.{Matched, Unmatched}
import compliance.engine.models.{PolicyViolationNotice, PolicyViolationNoticeType, VehicleEventPolicyMatch}
import compliance.engine.process.ProcessSpeedCompliance.VIOLATION_STATE_DESCRIPTOR
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import java.util.UUID

class ProcessSpeedCompliance extends KeyedProcessFunction[(UUID, UUID), VehicleEventPolicyMatch, PolicyViolationNotice] {
  @transient private var violationState: ValueState[UUID] = _

  override def open(parameters: Configuration): Unit = {
    violationState = getRuntimeContext.getState(VIOLATION_STATE_DESCRIPTOR)
  }

  def processElement(
                      policyMatch: VehicleEventPolicyMatch,
                      ctx: KeyedProcessFunction[(UUID, UUID), VehicleEventPolicyMatch, PolicyViolationNotice]#Context,
                      out: Collector[PolicyViolationNotice]
                    ): Unit = {
    policyMatch.matchType match {
      case Matched =>
      case Unmatched =>
        Option(violationState.value()).foreach { violatedRuleId =>
          out.collect(
            PolicyViolationNotice(
              policyId = ctx.getCurrentKey._1,
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