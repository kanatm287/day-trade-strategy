package fiveMinute

import java.sql.Timestamp


case class PreProcessParams(date: Timestamp,
                            ticker: String,
                            session_period: Int,
                            strategy: String,
                            benchmark_possibles_dates: Option[Array[Timestamp]],
                            volume_delta_possibles: Option[Array[Double]],
                            open_delta_possibles: Option[Array[Double]],
                            benchmark_delta_possibles: Option[Array[Double]],
                            non_outliers_possibles_dates: Option[Array[Timestamp]],
                            high_delta_exceptions: Option[String],
                            high_stop_exceptions: Option[String],
                            low_delta_exceptions: Option[String],
                            low_stop_exceptions: Option[String],
                            high_delta_exceptions_dates: Option[Array[Timestamp]],
                            high_stop_exceptions_dates: Option[Array[Timestamp]],
                            low_delta_exceptions_dates: Option[Array[Timestamp]],
                            low_stop_exceptions_dates: Option[Array[Timestamp]])
