package au.org.healthdirect.nhsd.codingchallenge

import org.apache.spark.sql.{Dataset, Encoder}

object DataSetUtils {
  implicit class DatasetOps[T](ds: Dataset[T]) {
    def collectP[U](pf: PartialFunction[T, U])(implicit enc: Encoder[U]): Dataset[U] = {
      ds.flatMap(pf.lift(_))
    }
  }
}
