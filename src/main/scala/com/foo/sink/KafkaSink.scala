package com.foo.sink

import java.util.Properties

import com.foo.datatype.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011


object KafkaSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    env.setParallelism(1)
    val serord = env.readTextFile("D:\\IDEA\\flink\\src\\main\\resources\\sensor.txt")
      .map(data => {
        val str = data.split(",")
        SensorReading(str(0).trim, str(1).trim.toLong, str(2).trim.toDouble).toString
      })
    val prop = new Properties()
    prop.setProperty("bootstrap.servers","hadoop102:9092")
    prop.setProperty("group.id","consumer-group")
    prop.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("auto.offset.reset","latest")

    serord.addSink(new FlinkKafkaProducer011[String]("seror",new SimpleStringSchema(),prop))

    serord.print()

    env.execute();
  }

}
