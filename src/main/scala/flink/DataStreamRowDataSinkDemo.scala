package flink

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.cfg.DorisSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.*;



object DataStreamRowDataSinkDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val soruce1 = env.fromElements("")
      .map(new MapFunction[String, RowData] {
        override def map(value: String): RowData = {
          val data = new GenericRowData(4)
          data.setField(0, 44);
          data.setField(1, 44);
          data.setField(2, "liuxiaoshuai_flink");
          data.setField(3, 44L);
          data
        }
      })

    val types = Array[LogicalType](new IntType(), new SmallIntType(), new VarCharType(), new BigIntType())
    val cols_name = Array[String]("siteid", "citycode", "username", "pv")
//    soruce1.addSink(
//      DorisSink.sink(cols_name,types,
//                DorisReadOptions.builder().build(),
//                DorisExecutionOptions.builder()
//                  .setBatchSize(3)
//                  .setBatchIntervalMs(0L)
//                  .setMaxRetries(3)
//                  .build(),
//                DorisOptions.builder()
//                  .setFenodes("test:8030")
//                  .setUsername("test")
//                  .setPassword("123456")
//                  .setTableIdentifier("city_info")
//      )
//    )

    env.execute()

  }
}
