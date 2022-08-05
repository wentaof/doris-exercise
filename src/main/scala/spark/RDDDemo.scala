package spark

import org.apache.spark.sql.SparkSession

object RDDDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SQLDemo").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    import org.apache.doris.spark._
    val doris_RDD = sc.dorisRDD(
      tableIdentifier = Some("test_db.table1"),
      cfg = Some(Map(
        "doris.fenodes" -> "test:8030",
        "doris.request.auth.user" -> "test",
        "doris.request.auth.password" -> "123456"
      ))
    )
    doris_RDD.collect().foreach(println)


  }
}
