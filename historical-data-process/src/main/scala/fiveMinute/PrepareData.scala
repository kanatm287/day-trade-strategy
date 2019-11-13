package fiveMinute


class PrepareData() extends VolumeDelta with SessionPeriod with OpenDelta with DayMax with DayMin with PrevDayClose {

    def generate(tickerParams: TickerParams) = {

//      Volume Delta Block

      volumeDelta("volume_delta", tickerParams)

//      Session Period

      sessionPeriod("session_period", tickerParams)

//      Day Open Block

      tickerParams.setDataset(openDelta("open_delta", tickerParams.dataset()))

//      Day Max

      dayMax("day_max", tickerParams)

//      Day Min

      dayMin("day_min", tickerParams)

//      Previous Day Close Block

      prevDayClose("prev_day_close", tickerParams)
  }
}
