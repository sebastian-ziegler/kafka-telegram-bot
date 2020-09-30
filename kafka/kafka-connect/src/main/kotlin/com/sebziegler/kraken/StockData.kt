package com.sebziegler.kraken

import kotlinx.serialization.Serializable

@Serializable
data class StockData(
    val currency: String,
    val askPrice: String, //a[0] AS ASK_PRICE,
    val bidPrice: String, //b[0] AS BID_PRICE,
    val lastTrade: String, //c[0] AS LAST_TRADE,
    val lastTradeVolume: String, //c[1] AS LAST_TRADE_VOLUME,
    val numberOfTradesToday: Int, //t[0] AS NUMBER_OF_TRADES_TODAY,
    val numberOfTrades24h: Int, //t[1] AS NUMBER_OF_TRADES_24H,
    val lowPriceToday: String, //l[0] AS LOW_PRICE_TODAY,
    val lowPrice24h: String, //l[1] AS LOW_PRICE_24H,
    val highPriceToday: String, //h[0] AS HIGH_PRICE_TODAY,
    val highPrice24h: String, //h[1] AS HIGH_PRICE_24H,
    val todayOpeningPrice: String //o AS TODAY_OPENING_PRICE
)
