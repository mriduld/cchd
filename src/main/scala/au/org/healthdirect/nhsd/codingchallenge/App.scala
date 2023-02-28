package au.org.healthdirect.nhsd.codingchallenge

import org.apache.spark.sql._

object App {
  def main(args: Array[String]) {
    val session = SparkSession.builder.appName("Healthcare Service Aggregator").getOrCreate()
    import session.implicits._

    if (args.length != 1) {
      sys.error("Expected input path for events")
    }
    val rawEventPath = args(0)
    val rawEvents: Dataset[RawEvent] = session.read.json(rawEventPath).as[RawEvent]

    val result = EventHandler.processEvents(session, rawEvents)

    result.pharmacies.write.mode("overwrite").json("output/pharmacies.jsonl")
    result.gps.write.mode("overwrite")json("output/gps.jsonl")
    result.errors.write.mode("overwrite").json("output/errors.jsonl")

    session.stop()
  }
}
