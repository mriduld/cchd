package au.org.healthdirect.nhsd.codingchallenge

import org.apache.spark.sql.SparkSession

object SparkTestUtils {
  def withSpark[T](f: SparkSession => T): T = {
    val session = new SparkSession.Builder()
      .appName("test")
      .config("spark.default.parallelism", 2)
      .config("spark.sql.shuffle.partitions", 1)
      .master("local")
      .getOrCreate()
    try {
      f(session)
    } finally {
      session.stop()
      session.close()
    }
  }
}
