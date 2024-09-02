package compliance.engine.window.time

import compliance.engine.models.VehiclePolicyMatchUpdate
import compliance.engine.traits.Loggable
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

case class TimeViolationTrigger() extends Trigger[VehiclePolicyMatchUpdate, TimeWindow] with Loggable {
  def onElement(element: VehiclePolicyMatchUpdate, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult =
    if (element.matched) {
      TriggerResult.CONTINUE
    } else {
      TriggerResult.FIRE_AND_PURGE
    }

  def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

  def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

  def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}

  override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit = {

  }

  override def canMerge: Boolean = true
}
