(ns screener.consts)

(def ^:const CONTENT-ID "div#screener-content")

(def ^:const CONTENT-TABLE "table[width=100%][cellpadding=10][cellspacing=0][border=0]")

(def ^:const TABLE "tbody > tr > td")

(def ^:const DAY-TRADE-SPY-URL "http://finviz.com/quote.ashx?t=spy")

(def ^:const DAY-TRADE-SPY-TABLE
  "table[width=100%][cellpadding=3][cellspacing=0][border=0][class=snapshot-table2]")

(def ^:const DAY-TRADE-MARKET-CHANGE-UP
  (str
    "http://finviz.com/screener.ashx?v=411&f=ind_stocksonly,sh_avgvol_o1000,"
    "sh_price_o30,ta_averagetruerange_o1,ta_change_u&ft=4"))

(def ^:const DAY-TRADE-MARKET-CHANGE-DOWN
  (str
    "http://finviz.com/screener.ashx?v=411&f=ind_stocksonly,sh_avgvol_o1000,"
    "sh_price_o30,ta_averagetruerange_o1,ta_change_d&ft=4"))

(def ^:const DAY-TRADE-MAIN
  ["http://finviz.com/screener.ashx?v=411&f=ind_stocksonly,sh_avgvol_o1000,sh_price_o30,sh_relvol_o"
   ",ta_averagetruerange_o0.5&ft=4"])

(def ^:const DAY-TRADE-NEW-HIGH
  ["http://finviz.com/screener.ashx?v=411&f=ind_stocksonly,sh_avgvol_o1000,sh_price_o30,sh_relvol_o"
   ",ta_averagetruerange_o0.5,ta_highlow52w_nh&ft=4"])

(def ^:const DAY-TRADE-NEW-LOW
  ["http://finviz.com/screener.ashx?v=411&f=ind_stocksonly,sh_avgvol_o1000,sh_price_o30,sh_relvol_o"
   ",ta_averagetruerange_o0.5,ta_highlow52w_nl&ft=4"])
