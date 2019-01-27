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

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import com.skt.skon.wordcount.config.WordCountConfiguration

case class WordWithCount(word: String, count: Int)

object WordCount {

  def main(args: Array[String]) {
    // configuration by arguemtnt
    val wordcountConfiguration = WordCountConfiguration.get(args, "Word Count on Flink")

    // Kafka properties
    val consumerProperties = new Properties()
    consumerProperties.put("bootstrap.server", wordcountConfiguration.kafkaConsumerServer.mkString(","))
    consumerProperties.put("group.id", wordcountConfiguration.kafkaConsumerGroupID)

    val producerProperties = new Properties()
    producerProperties.put("broker.list", wordcountConfiguration.kafkaProducerServer.mkString(","))

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val textStream = env.socketTextStream("localhost", 9000, '\n')

    val wordStream = textStream
      .flatMap { line => line.split("\\s") }
      .map { word => WordWithCount(word, 1) }
      .name("split-text-to-words")
      .uid("split-text-to-words")

    val wordCount = wordStream
      .keyBy( _.word )
      .timeWindow(Time.seconds(5))
      .sum("count")

    wordCount.print()
      .setParallelism(1)
      .name("count-words")
      .uid("count-words")

    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }

}
