package au.org.healthdirect.nhsd.codingchallenge

import org.apache.spark.sql.SparkSession

object SparkTestUtils {
  def withSpark[T](f: SparkSession => T): T = {
    val session = new SparkSession.Builder()
      .appName("test")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", 2)
      .getOrCreate()
    try {
      f(session)
    } finally {
      session.close()
    }
  }
}
