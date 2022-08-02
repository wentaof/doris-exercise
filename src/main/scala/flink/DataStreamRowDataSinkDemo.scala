package flink

import org.apache.doris.flink.cfg.{DorisExecutionOptions, DorisOptions, DorisReadOptions, DorisSink}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.environment._
import org.apache.flink.table.data.{GenericRowData, RowData}
import org.apache.flink.table.types.logical.{BigIntType, LogicalType, SmallIntType, VarCharType}
import org.apache.parquet.format.IntType


object DataStreamRowDataSinkDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val soruce1 = env.fromElements()
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
    soruce1.addSink(
      DorisSink.sink(cols_name,types,
                DorisReadOptions.builder().build(),
                DorisExecutionOptions.builder()
                  .setBatchSize(3)
                  .setBatchIntervalMs(0L)
                  .setMaxRetries(3)
                  .build(),
                DorisOptions.builder()
                  .setFenodes("test:8030")
                  .setUsername("test")
                  .setPassword("123456")
                  .setTableIdentifier("city_info")
      )
    )

    env.execute()

  }
}
