package com.skt.skon.wordcount.trigger

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.ReducingStateDescriptor
import org.apache.flink.api.common.typeutils.base.LongSerializer
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow


class BurstEventTimeTrigger[T](burst: Long = 5) extends Trigger[T, TimeWindow] {

  private val countDescriptor = new ReducingStateDescriptor[Long](
    "count", new BurstEventTimeTrigger[T].Sum, LongSerializer.INSTANCE)

  override def onElement(element: T, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    val count = ctx.getPartitionedState(countDescriptor)

    count.add(1L)
    if (count.get == burst) {
      return TriggerResult.FIRE
    }
    TriggerResult.CONTINUE
  }

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    if (time == window.maxTimestamp) TriggerResult.FIRE else TriggerResult.CONTINUE
  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def canMerge: Boolean = true

  override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit = {
    ctx.mergePartitionedState(countDescriptor)
    if (window.maxTimestamp > ctx.getCurrentWatermark)
      ctx.registerEventTimeTimer(window.maxTimestamp)
  }

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    ctx.getPartitionedState(countDescriptor).clear()
    ctx.deleteEventTimeTimer(window.maxTimestamp)
  }

  override def toString: String = s"BurstEventTimeTrigger(burst=$burst)"

  @SerialVersionUID(1L)
  private class Sum extends ReduceFunction[Long] {
    @throws[Exception]
    override def reduce(value1: Long, value2: Long): Long = value1 + value2
  }

}