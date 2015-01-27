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
 * Created by mbautin on 1/26/15.
 */
object LoadTweetTermCounts {

  private[this] val WHITESPACE_RE = """\s+""".r
  private[this] val ACCEPTABLE_TERM_RE = """^#?[A-Za-z0-9_-]+$""".r

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Twitter")
    val ssc = new StreamingContext(sparkConf, Seconds(15))

//    def createStream(
//      ssc: StreamingContext,
//      twitterAuth: Option[Authorization],
//      filters: Seq[String] = Nil,
//      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
//      ): ReceiverInputDStream[Status] = {
//      new TwitterInputDStream(ssc, twitterAuth, filters, storageLevel)
//    }

    val cb = new ConfigurationBuilder()
    val twitterCredentials =
      scala.io.Source.fromFile("/home/mbautin/twitter_credentials.txt").getLines().toArray
    cb.setOAuthConsumerKey(twitterCredentials(0))
    cb.setOAuthConsumerSecret(twitterCredentials(1))
    cb.setOAuthAccessToken(twitterCredentials(2))
    cb.setOAuthAccessTokenSecret(twitterCredentials(3))

    val tf = new TwitterFactory(cb.build())
    val twitter = tf.getInstance()

    val tweets = TwitterUtils.createStream(ssc,
      twitterAuth = Some(twitter.getAuthorization),
      filters = Seq())

    val termCounts = tweets.flatMap { tweet =>
      val createdAt = tweet.getCreatedAt
      val sec = createdAt.getSeconds()

      // Round to 15-second buckets.
      val roundedSec = sec - sec % 15
      val roundedCreatedAt: Date = createdAt.clone.asInstanceOf[Date]
      roundedCreatedAt.setSeconds(roundedSec)

      val place = Option(tweet.getPlace)
      WHITESPACE_RE.split(tweet.getText.trim).flatMap { term =>
        if (term.length >= 4 && ACCEPTABLE_TERM_RE.findFirstIn(term).isDefined) {
          Some((
            (
              new DateTime(roundedCreatedAt.getTime),
              term.toLowerCase,
              place.map(_.getFullName).orNull,
              place.map(_.getCountry).orNull
            ), 1L
          ))
        } else {
          None
        }
      }
    }.reduceByKey(_ + _)

    val hadoopConf = new Configuration()
    hadoopConf.set("hbase.zookeeper.quorum", "10.1.1.153")
    val phoenixPigConfiguration = new PhoenixPigConfiguration(hadoopConf)
    phoenixPigConfiguration.setServerName("10.1.1.153")
    phoenixPigConfiguration.setTableName("tweets_15min_counts")
    phoenixPigConfiguration.setSchemaType(SchemaType.QUERY)

    termCounts.map { case ((ts, term, place, country), count) =>
      val phoenixRecord = new PhoenixRecord()
      phoenixRecord.add(ts)
      phoenixRecord.add(term)
      phoenixRecord.add(place)
      phoenixRecord.add(country)
      phoenixRecord.add(count)
      (null, phoenixRecord)
    }.saveAsNewAPIHadoopFiles(
      prefix = "tweet_stats",
      suffix = ".dat",
      keyClass = classOf[NullWritable],
      valueClass = classOf[PhoenixRecord],
      outputFormatClass = classOf[PhoenixOutputFormat],
      conf = hadoopConf
    )

    ssc.start()
    ssc.awaitTermination()

//    val sparkConf = new SparkConf()
//    sparkConf.setMaster("local[4]")
//    sparkConf.setAppName(getClass.getSimpleName)
//    val sc = new SparkContext(sparkConf)
//    val conf = new Configuration()
//    conf.set("hbase.zookeeper.quorum", "10.1.1.153")
//
//    val phoenixConf = new PhoenixPigConfiguration(conf)
//    phoenixConf.setServerName("10.1.1.153")
//    phoenixConf.setSchemaType(SchemaType.QUERY)
//
//    val prdd =
//      PhoenixRDD.NewPhoenixRDD(sc, "CSD_TEST1", Seq("C_CATEGORY", "C_CHAIN", "C_COUNTY", "C_CITY", "C_INFO"),
//        predicate = None, conf)
//    prdd.collect().map { row =>
//      println(row)
//    }


  }
}
