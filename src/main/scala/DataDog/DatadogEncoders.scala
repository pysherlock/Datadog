package DataDog

import org.apache.spark.sql.{Encoders, Encoder}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

object DatadogEncoders {

  implicit def datadogIntEncoder: Encoder[Int] = Encoders.scalaInt

  implicit def datadogIntArrayEncoder = ExpressionEncoder[Array[Int]]

}
