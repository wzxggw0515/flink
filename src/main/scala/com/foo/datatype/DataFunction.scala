package com.foo.datatype
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
case class SensorReading(id:String,timestamp:Long,temperature:Double)
object DataFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    import org.apache.flink.api.scala._
    val datas = env.readTextFile("D:\\IDEA\\flink\\src\\main\\resources\\sensor.txt")
      .map(data => {
        val str = data.split(",")
        SensorReading(str(0).trim, str(1).trim.toLong, str(2).trim.toDouble)
      })
    datas.filter(a=>a.id.startsWith("sensor_1")).print()

    env.execute();
  }
   class MyFilter()extends FilterFunction[SensorReading]{
     override def filter(t: SensorReading): Boolean = {
       t.temperature.>(35)
     }
   }

}
