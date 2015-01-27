package com.simplymeasured.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.phoenix.pig.PhoenixPigConfiguration
import org.apache.phoenix.pig.PhoenixPigConfiguration.SchemaType
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by mbautin on 1/26/15.
 */
object PhoenixRDDManualTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[4]")
    sparkConf.setAppName(getClass.getSimpleName)
    val sc = new SparkContext(sparkConf)
    val conf = new Configuration()
    conf.set("hbase.zookeeper.quorum", "10.1.1.153")

    val phoenixConf = new PhoenixPigConfiguration(conf)
    phoenixConf.setServerName("10.1.1.153")
    phoenixConf.setSchemaType(SchemaType.QUERY)

    val prdd =
      PhoenixRDD.NewPhoenixRDD(sc, "CSD_TEST1", Seq("C_CATEGORY", "C_CHAIN", "C_COUNTY", "C_CITY", "C_INFO"),
        predicate = None, conf)
    prdd.collect().map { row =>
      println(row)
    }
  }
}
