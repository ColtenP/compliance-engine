package compliance.engine.window.speed

import compliance.engine.models.SpeedPolicyMatchUpdate
import compliance.engine.traits.Loggable
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

case class SpeedViolationTrigger() extends Trigger[SpeedPolicyMatchUpdate, TimeWindow] with Loggable {
  def onElement(element: SpeedPolicyMatchUpdate, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    if (!element.matched) {
      TriggerResult.FIRE_AND_PURGE
    } else if (element.ruleMaximum.exists(_ <= element.speed) || element.ruleMinimum.exists(_ >= element.speed)) {
      TriggerResult.CONTINUE
    } else {
      TriggerResult.FIRE_AND_PURGE
    }
  }

  def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

  def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

  def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}

  override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit = {}

  override def canMerge: Boolean = true
}
