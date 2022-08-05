package flink

import org.apache.doris.flink.cfg._;
import org.apache.doris.flink.datastream.DorisSourceFunction;
import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import
org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.Properties

object DataStreamJsonSinkDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val pro = new Properties()
    pro.setProperty("format", "json")
    pro.setProperty("strip_outer_array", "true")

//    env.fromElements("{\"longitude\": \"116.405419\", \"city\": \"北京\", \"latitude\": \"39.916927\"}")
//      .addSink(DorisSink.sink(
//        DorisReadOptions.builder().build(),
//        DorisExecutionOptions.builder()
//          .setBatchSize(3)
//          .setBatchIntervalMs(0L)
//          .setMaxRetries(3)
//          .setStreamLoadProp(pro)
//          .build(),
//        DorisOptions.builder()
//          .setFenodes("test:8030")
//          .setUsername("test")
//          .setPassword("123456")
//          .setTableIdentifier("city_info")
//      ))

//        .addSink(
//          DorisSink.sink(DorisOptions.builder()
//            .setFenodes("test:8030")
//            .setUsername("test")
//            .setPassword("123456")
//            .setTableIdentifier("city_info"))
//        )

        env.execute()
  }
}
