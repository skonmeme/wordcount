/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.skt.skon.wordcount

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.util.Collector
import org.json4s.DefaultFormats
import org.json4s.native.Serialization
import com.skt.skon.wordcount.config.WordCountConfiguration
import com.skt.skon.wordcount.trigger.BurstProcessingTimeTrigger

case class WordWithCount(word: String, count: Int)

object WordCount {

  def main(args: Array[String]) {
    // tags
    val burst = 5
    val burstOutputTag = OutputTag[String]("burst-output")

    // configuration by argument
    val wordcountConfigurations = WordCountConfiguration.get(args, "flink run -c com.skt.skon.wordcount.WordCount wordcount.jar")

    // Kafka properties
    val consumerProperties = new Properties()
    consumerProperties.put("bootstrap.servers", wordcountConfigurations.kafkaConsumerServers.mkString(","))
    consumerProperties.put("group.id", wordcountConfigurations.kafkaConsumerGroupID)

    val producerProperties = new Properties()
    producerProperties.put("bootstrap.servers", wordcountConfigurations.kafkaProducerServers.mkString(","))

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // no event time
    //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val textStream = env.addSource(
        new FlinkKafkaConsumer[String](
          wordcountConfigurations.kafkaConsumerTopic,
          new SimpleStringSchema(),
          consumerProperties))
      .setParallelism(1)
      .name("source-from-kafka")
      .uid("source-from-kafka-uid")

    val wordStream = textStream
      .flatMap { line => line.split("\\s") }
      .map { word => WordWithCount(word, 1) }
      .setParallelism(1)
      .name("split-text-to-words")
      .uid("split-text-to-words")

    val wordCountStream = wordStream
      .keyBy( _.word )
      .timeWindow(Time.seconds(60))
      .trigger(new BurstProcessingTimeTrigger[WordWithCount](burst))
      .sum("count")
      .setParallelism(2)
      .name("count-words")
      .uid("count-words-uid")
      .process(new ProcessFunction[WordWithCount, WordWithCount] {
        override def processElement(value: WordWithCount, ctx: ProcessFunction[WordWithCount, WordWithCount]#Context, out: Collector[WordWithCount]): Unit = {
          if (value.count == burst + 1) {
            // emit data to side output
            ctx.output(burstOutputTag, s"${value.word}: bursted")
          } else {
            // emit data to regular output
            out.collect(value)
          }
        }
      })

    wordCountStream
      .map(w => Serialization.write(w)(DefaultFormats))
      .setParallelism(2)
      .addSink({
        val producer = new FlinkKafkaProducer[String](
          wordcountConfigurations.kafkaProducerTopic,
          new SimpleStringSchema,
          producerProperties)
        producer.setWriteTimestampToKafka(true)
        producer
      })
      .name("kafka-sink")
      .uid("kafka-sink-uid")

    wordCountStream.getSideOutput(burstOutputTag)
      .map(w => Serialization.write(w)(DefaultFormats))
      .setParallelism(2)
      .addSink({
        val producer = new FlinkKafkaProducer[String](
          wordcountConfigurations.kafkaPorducerAnotherTopic,
          new SimpleStringSchema,
          producerProperties)
        producer.setWriteTimestampToKafka(true)
        producer
      })
      .name("kafka-burst-sink")
      .uid("kafka-busrt-sink-uid")

    wordCountStream
      .print()
      .setParallelism(1)
      .name("console-sink")
      .uid("console-sink-uid")

    // execute program
    env.execute("Word Count on Flink")
  }

}
