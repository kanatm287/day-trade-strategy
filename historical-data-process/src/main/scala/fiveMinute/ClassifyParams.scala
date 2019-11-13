package fiveMinute

import java.sql.Timestamp

case class ClassifyParams(date: Timestamp,
                          ticker: String,
                          session_period: Int,
                          strategy: String,
                          high_delta_classifier: Option[Array[Byte]],
                          high_stop_classifier: Option[Array[Byte]],
                          low_delta_classifier: Option[Array[Byte]],
                          low_stop_classifier: Option[Array[Byte]])
