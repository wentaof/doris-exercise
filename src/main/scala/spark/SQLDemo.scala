package spark

import org.apache.spark.sql.SparkSession
/*
CREATE TABLE table1
(
 siteid INT DEFAULT '10',
 citycode SMALLINT,
 username VARCHAR(32) DEFAULT '',
 pv BIGINT SUM DEFAULT '0'
)
AGGREGATE KEY(siteid, citycode, username)
DISTRIBUTED BY HASH(siteid) BUCKETS 10
PROPERTIES("replication_num" = "1");
insert into table1 values
(1,1,'jim',2),
(2,1,'grace',2),
(3,2,'tom',2),
(4,3,'bush',3),
(5,3,'helen',3);
 */
object SQLDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SQLDemo").master("local[*]").getOrCreate()
    spark.sql(
      """
        |create temporary view spark_doris using doris
        |options(
        |"table.identifier"="test_db.table1",
        |"fenodes"="test:8030",
        |"user"="test",
        |"password"="123456"
        |)
        |""".stripMargin)
    //读取数据
    spark.sql("select * from spark_doris").show()
    //写入数据
    spark.sql("insert into spark_doris values (9,9,'xiaoshuai',5)")
  }
}
