import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Streamcount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val socketext = env.socketTextStream("192.168.245.151",1086)
    val streamtest=socketext.flatMap(_.split(" "))
      .map((_,1)).keyBy(0).sum(1)
    streamtest.print()

    env.execute("StreamingScalaWordCount")

  }
}