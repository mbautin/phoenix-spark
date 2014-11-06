/*
   Copyright 2014 Simply Measured, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package com.simplymeasured.spark

import java.sql.DriverManager

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.phoenix.schema.PDataType
import org.apache.phoenix.util.ColumnInfo
import org.apache.spark.sql.catalyst.types.{StructField, StringType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class PhoenixRDDTest extends FunSuite with Matchers with BeforeAndAfterAll {
  lazy val hbaseTestingUtility = {
    new HBaseTestingUtility()
  }

  lazy val hbaseConfiguration = {
    hbaseTestingUtility.getConfiguration
  }

  lazy val quorumAddress = {
    hbaseConfiguration.get("hbase.zookeeper.quorum")
  }

  lazy val zookeeperClientPort = {
    hbaseConfiguration.get("hbase.zookeeper.property.clientPort")
  }

  lazy val zookeeperZnodeParent = {
    hbaseConfiguration.get("zookeeper.znode.parent")
  }

  lazy val hbaseConnectionString = {
    s"$quorumAddress:$zookeeperClientPort:$zookeeperZnodeParent"
  }

  override def beforeAll() {
    hbaseTestingUtility.startMiniCluster()

    val conn = DriverManager.getConnection(s"jdbc:phoenix:$hbaseConnectionString")

    // each SQL statement used to set up Phoenix must be on a single line. Yes, that
    // can potentially make large lines.
    val setupSqlSource = getClass.getClassLoader.getResourceAsStream("setup.sql")

    val setupSql = scala.io.Source.fromInputStream(setupSqlSource).getLines()

    for (sql <- setupSql) {
      val stmt = conn.createStatement()

      stmt.execute(sql)

      stmt.close()
    }

    conn.commit()
  }

  override def afterAll() {
    hbaseTestingUtility.shutdownMiniCluster()
  }

  val conf = new SparkConf()

  val sc = new SparkContext("local", "PhoenixSparkTest", conf)

  test("Can create valid SQL") {
    val rdd = PhoenixRDD.NewPhoenixRDD(sc, "localhost", "MyTable", Array("Foo", "Bar"),
      conf = new Configuration())

    assert(rdd.buildSql == "SELECT \"Foo\", \"Bar\" FROM \"MyTable\"")
  }

  test("Can convert Phoenix schema") {
    val phoenixSchema = List(
      new ColumnInfo("varcharColumn", PDataType.VARCHAR.getSqlType)
    )

    val rdd = PhoenixRDD.NewPhoenixRDD(sc, "localhost", "MyTable", Array("Foo", "Bar"),
      conf = new Configuration())

    val catalystSchema = rdd.phoenixSchemaToCatalystSchema(phoenixSchema)

    val expected = List(StructField("varcharColumn", StringType, nullable = true))

    catalystSchema shouldEqual expected
  }

  test("Can create schema RDD and execute query") {
    val sqlContext = new SQLContext(sc)

    val rdd1 = PhoenixRDD.NewPhoenixRDD(sc, hbaseConnectionString,
      "TABLE1", Array("ID", "COL1"), conf = new Configuration())

    val schemaRDD1 = rdd1.toSchemaRDD(sqlContext)

    schemaRDD1.registerTempTable("sql_table_1")

    val rdd2 = PhoenixRDD.NewPhoenixRDD(sc, hbaseConnectionString,
      "TABLE2", Array("ID", "TABLE1_ID", "col1"),
      conf = new Configuration())

    val schemaRDD2 = rdd2.toSchemaRDD(sqlContext)

    schemaRDD2.registerTempTable("sql_table_2")

    val sqlRdd = sqlContext.sql("SELECT t1.ID AS t1_id, t1.COL1 AS t1_col1, t2.ID AS t2_id, t2.TABLE1_ID, t2.col1 AS t2_col1 FROM sql_table_1 AS t1 INNER JOIN sql_table_2 AS t2 ON (t2.TABLE1_ID = t1.ID)")

    val count = sqlRdd.count()

    count shouldEqual 6L
  }
}