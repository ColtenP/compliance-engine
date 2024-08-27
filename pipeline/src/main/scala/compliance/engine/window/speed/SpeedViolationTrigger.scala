package compliance.engine.window.speed

import compliance.engine.models.VehicleEventPolicyMatch
import compliance.engine.traits.Loggable
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

case class SpeedViolationTrigger() extends Trigger[VehicleEventPolicyMatch, TimeWindow] with Loggable {
  def onElement(element: VehicleEventPolicyMatch, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    val ruleInViolation = element.policy.findFirstRuleInViolation(element.vehicleEvent)

    ruleInViolation match {
      case Some(_) => TriggerResult.CONTINUE
      case None => TriggerResult.FIRE_AND_PURGE
    }
  }

  def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

  def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

  def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}

  override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit = {

  }

  override def canMerge: Boolean = true
}
