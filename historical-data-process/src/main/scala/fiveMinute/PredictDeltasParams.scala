package fiveMinute

import java.sql.Timestamp


case class PredictDeltasParams (date: Timestamp,
                                ticker: String,
                                high_delta: Option[Boolean],
                                low_delta: Option[Boolean],
                                high_stop: Option[Boolean],
                                low_stop: Option[Boolean])
