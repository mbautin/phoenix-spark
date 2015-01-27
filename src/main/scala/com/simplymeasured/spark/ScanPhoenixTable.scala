package com.simplymeasured.spark

import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.phoenix.pig.PhoenixPigConfiguration
import org.apache.phoenix.pig.PhoenixPigConfiguration.SchemaType
import org.apache.phoenix.pig.hadoop.{PhoenixOutputFormat, PhoenixRecord}
import org.apache.pig.ResourceSchema.ResourceFieldSchema
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import twitter4j.TwitterFactory
import twitter4j.auth.Authorization
import twitter4j.conf.ConfigurationBuilder
import org.apache.spark.streaming.StreamingContext._

/**
 * Created by mbautin on 1/27/15.
 */
object ScanPhoenixTable {

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
      PhoenixRDD.NewPhoenixRDD(sc, "TWEETS_SINK_4", Seq("TS", "TERM", "PLACE", "COUNTRY", "COUNT"),
        predicate = None, conf)
    prdd.collect().map { row =>
      println(new DateTime(row(0).asInstanceOf[java.sql.Time].getTime))
    }
  }
}
