package com.skt.skon.wordcount.trigger

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.ReducingStateDescriptor
import org.apache.flink.streaming.api.windowing.triggers.{EventTimeTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class BurstProcessingTimeTrigger[T](burst: Int) extends Trigger[T, TimeWindow] {

  private val countDescriptor = new ReducingStateDescriptor[Int](
    "count", new ReduceFunction[Int] {
      override def reduce(value1: Int, value2: Int): Int = value1 + value2
    }, classOf[Int])

  override def onElement(element: T, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    val count = ctx.getPartitionedState(countDescriptor)

    count.add(1)
    ctx.registerProcessingTimeTimer(window.maxTimestamp)
    if (count.get == burst + 1) {
      TriggerResult.FIRE
    } else {
      TriggerResult.CONTINUE
    }
  }

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE

  override def canMerge: Boolean = true

  override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit = {
    ctx.mergePartitionedState(countDescriptor)
    if (window.maxTimestamp > ctx.getCurrentProcessingTime)
      ctx.registerProcessingTimeTimer(window.maxTimestamp)
  }

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    ctx.getPartitionedState(countDescriptor).clear()
    ctx.deleteProcessingTimeTimer(window.maxTimestamp)
  }

  override def toString: String = s"BurstEventTimeTrigger(burst=$burst)"

}
