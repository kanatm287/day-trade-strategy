package fiveMinute


case class TestParams(benchmark_negative: Option[Double],
                      benchmark_positive: Option[Double],
                      limit_price: Double,
                      limit_time: Int,
                      limit_trades: Int,
                      min_trail_percent: Double,
                      stop_trades: Int,
                      trail_percent: Double,
                      trail_time: Int,
                      high_delta: Option[Boolean],
                      low_delta: Option[Boolean],
                      low_stop: Option[Boolean],
                      high_stop: Option[Boolean],
                      spy: Boolean)
