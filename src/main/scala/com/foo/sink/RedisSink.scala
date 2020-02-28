package com.foo.sink

import com.foo.datatype.SensorReading
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RediSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val conf = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPort(6379).build()
    import org.apache.flink.api.scala._
    val serord = env.readTextFile("D:\\IDEA\\flink\\src\\main\\resources\\sensor.txt")
      .map(data => {
        val str = data.split(",")
        SensorReading(str(0).trim, str(1).trim.toLong, str(2).trim.toDouble)
      })
    serord.addSink(new RedisSink[SensorReading](conf,new myredismapper))
    serord.print()
    env.execute()
  }
  class myredismapper extends  RedisMapper[SensorReading] {
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature")
    }

    override def getKeyFromData(t: SensorReading): String = t.id.toString

    override def getValueFromData(t: SensorReading): String = t.temperature.toString
  }

}
