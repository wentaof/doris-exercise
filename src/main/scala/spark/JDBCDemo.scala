package spark

import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Properties

object JDBCDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SQLDemo").master("local[*]").getOrCreate()

//    读取数据
    val df = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://test:9030/test_db")
      .option("user", "test")
      .option("password", "123456")
      .option("dbtable", "table1")
      .load()

    df.show()

//    写入数据
    import spark.implicits._
    val mockdata = List((22,3,"zhangsan",3),(22,3,"zhaosi",4),(22,3,"wangwu",5),(22,3,"zhaoliu",6))
      .toDF("siteid","citycode","username","pv")
    val prop = new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","123456")
    df.write.mode(SaveMode.Append)
      .jdbc("jdbc:mysql://test:9030/test_db",table="table1",prop)
  }
}
