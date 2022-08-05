package spark

import org.apache.spark.sql.SparkSession
//DataFrame 方式读写数据（batch）
object DataFrameDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SQLDemo").master("local[*]").getOrCreate()

    //读取数据 --失败
//    val table1 = spark.read.format("doris")
//      .option("doris.table.identifier", "test_db.table1")
//      .option("doris.fenodes", "test:8030")
//      .option("user", "test")
//      .option("password", "123456")
//      .load()
//    table1.show(false)

    //写入数据   --成功
    import spark.implicits._
    val mockdata = List((11,3,"zhangsan",3),(11,3,"zhaosi",4),(11,3,"wangwu",5),(11,3,"zhaoliu",6))
      .toDF("siteid","citycode","username","pv")
    mockdata.show()
    mockdata.write.format("doris")
      .option("doris.table.identifier", "test_db.table1")
      .option("doris.fenodes", "test:8030")
      .option("user", "test")
      .option("password", "123456")
//      指定写入的字段
//      .option("doris.write.fields", "username")
      .save()
  }
}
