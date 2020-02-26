package com.foo

import org.apache.flink.api.scala.ExecutionEnvironment

object wordcount {

  def main(args: Array[String]): Unit = {
   val env:ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
   val  input  = "\\opt\\module\\flink\\job\\input.txt"
   import  org.apache.flink.api.scala._
   val dataset:DataSet[String] = env.readTextFile(input);
   val aggr = dataset.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)
   aggr.print()
   aggr.writeAsCsv("\\opt\\module\\flink\\job\\output.txt")
   env.execute("batch ")



}

}
