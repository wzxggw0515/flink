package com.foo.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object KafkaSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val prop = new Properties()
    prop.setProperty("bootstrap.servers","hadoop102:9092")
    prop.setProperty("group.id","consumer-group")
    prop.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("auto.offset.reset","latest")
    import  org.apache.flink.api.scala._
    val kafkacou = env.addSource(new FlinkKafkaConsumer011[String]("seror",new SimpleStringSchema(),prop))
    kafkacou.addSink(new FlinkKafkaProducer011[String]("test",new SimpleStringSchema(),prop))

    kafkacou.print()

    env.execute("kafka-consumer");

  }


}
