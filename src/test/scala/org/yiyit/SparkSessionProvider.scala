package org.yiyit

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SparkSessionProvider extends BeforeAndAfterAll { this: Suite =>
  @transient lazy val spark: SparkSession = {
    SparkSession.builder()
      .master("local[2]")
      .appName("spark-test")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
      .config("spark.driver.extraJavaOptions", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }
}