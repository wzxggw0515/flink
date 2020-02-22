package word

import org.apache.flink.api.scala.ExecutionEnvironment

object wordcount {

  def main(args: Array[String]): Unit = {
   val env:ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
   val  input  = "D:\\tmp\\a.txt"
   import  org.apache.flink.api.scala._
   val dataset:DataSet[String] = env.readTextFile(input);
   val aggr = dataset.flatMap(_.split("")).map((_,1)).groupBy(0).sum(1)
  aggr.print();
}

}
