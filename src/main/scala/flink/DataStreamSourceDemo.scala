package flink

import org.apache.doris.flink.cfg.DorisStreamOptions
import org.apache.doris.flink.datastream.DorisSourceFunction
import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

import java.util.Properties

object DataStreamSourceDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val prop = new Properties()
    prop.setProperty("fenodes","test:8030")
    prop.setProperty("username","test")
    prop.setProperty("password","123456")
    prop.setProperty("table.identifier","test_db.table1")

    val value = env.addSource(new DorisSourceFunction(new DorisStreamOptions(prop), new SimpleListDeserializationSchema))
    value.print()

    env.execute()
  }
}
