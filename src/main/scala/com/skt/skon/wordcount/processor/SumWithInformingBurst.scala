package com.skt.skon.wordcount.processor

import com.skt.skon.wordcount.datatypes.WordWithCount
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class SumWithInformingBurst(burst: Long, burstOutputTag: OutputTag[String]) extends ProcessWindowFunction[WordWithCount, WordWithCount, String, TimeWindow] {

  lazy val fired: ValueState[Boolean] = getRuntimeContext.getState(
    new ValueStateDescriptor[Boolean]("fired", classOf[Boolean])
  )

  override def process(key: String, context: Context, elements: Iterable[WordWithCount], out: Collector[WordWithCount]): Unit = {
    val word = elements.head.word
    var count = 0L

    for (_ <- elements)
      count += 1
    if ((count == burst + 1) && !fired.value) {
      fired.update(true)
      context.output(burstOutputTag, s"$word: bursted")
    } else
      out.collect(WordWithCount(word, count))
  }

}