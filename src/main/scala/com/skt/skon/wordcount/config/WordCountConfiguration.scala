package com.skt.skon.wordcount.config

import java.io.File

import scopt.OptionParser

case class WordCountConfiguration(kafkaConsumerServers: Seq[String] = Seq("localhost:9092"),
                                  kafkaConsumerTopic: String = "wordcount-text",
                                  kafkaConsumerGroupID: String = "flink-wordcount",
                                  kafkaProducerServers: Seq[String] = Seq("localhost:9092"),
                                  kafkaProducerTopic: String = "wordcount-words",
                                  kafkaPorducerAnotherTopic: String = "wordcount-another"
                                 )

object WordCountConfiguration {

  def get(args: Array[String], programName: String): WordCountConfiguration = {
    var parser = new OptionParser[WordCountConfiguration](programName) {
      head(programName)

      help("help").text("prints this usage text")

      opt[Seq[String]]("kafka-consumer-server")
        .valueName("<host1:port1>,<host2:port2>...")
        .action((x, c) => c.copy(kafkaConsumerServers = x))
        .text("Kafka servers to load texts")

      opt[String]("kafka-consumer-topic")
        .action((x, c) => c.copy(kafkaConsumerTopic = x))
        .text("Kafka topic to load texts")

      opt[String]("kafka-consumer-group-id")
        .action((x, c) => c.copy(kafkaConsumerGroupID = x))
        .text("Consumer group ID to load texts")

      opt[Seq[String]]("kafka-producer-server")
        .valueName("<host1:port1>,<host2:port2>...")
        .action((x, c) => c.copy(kafkaProducerServers = x))
        .text("Kafka servers to print out words with counts")

      opt[String]("kafka-producer-topic")
        .action((x, c) => c.copy(kafkaProducerTopic = x))
        .text("Kafka topic to print out words with counts")

      opt[String]("kafka-another-topic")
        .action((x, c) => c.copy(kafkaPorducerAnotherTopic = x))
        .text("Kafka topic to print out additional information")

    }

    parser.parse(args, WordCountConfiguration()) match {
      case Some(c) => c
      case None => throw new RuntimeException("Failed to get a valid configuration object")
    }
  }

}
