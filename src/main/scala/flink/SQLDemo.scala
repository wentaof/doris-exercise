package flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object SQLDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)
    val create_sql=
      """
        |create table flink_doris(
        |siteid int,
        |citycode smallint,
        |username string,
        |pv bigint
        |)
        |with(
        |'connector' = 'doris',
        |'fenode' = 'test:8030',
        |'table.identifier' = 'test_db.table1',
        |'username' = 'test',
        |'password' = '123456'
        |)
        |""".stripMargin
    tableEnv.executeSql(create_sql)
//    读取数据
    tableEnv.executeSql("select * from flink_doris").print()
//    写入数据
    tableEnv.executeSql("insert into flink_doris(siteid,username,pv) values(33,'liuxiaoshuai2',100))")

  }
}
