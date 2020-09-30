package com.sebziegler.connector

import com.sebziegler.kraken.KrakenHandler
import com.sebziegler.kraken.StockData
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask
import java.time.Clock
import kotlin.reflect.full.declaredMemberProperties


class KrakenSourceTask: SourceTask() {

    private lateinit var topic: String
    private lateinit var apiKey: String
    private lateinit var apiSecret: String
    private lateinit var currencyList: List<String>
    private var pollingRate: Long = 5000L
    private var offset = 0L
    private val krakenHandler: KrakenHandler = KrakenHandler()
    private var startTime = Clock.systemDefaultZone().instant()
    private val krakenSchema = buildKrakenSchema()
    private val payloadSchema: Schema = SchemaBuilder.array(krakenSchema)

    private fun buildKrakenSchema(): Schema {
        return SchemaBuilder
            .struct()
            .name("com.sebziegler.kraken.StockData")
            .field("currency", Schema.STRING_SCHEMA)
            .field("askPrice", Schema.STRING_SCHEMA)
            .field("bidPrice", Schema.STRING_SCHEMA)
            .field("lastTrade", Schema.STRING_SCHEMA)
            .field("lastTradeVolume", Schema.STRING_SCHEMA)
            .field("numberOfTradesToday", Schema.INT32_SCHEMA)
            .field("numberOfTrades24h", Schema.INT32_SCHEMA)
            .field("lowPriceToday", Schema.STRING_SCHEMA)
            .field("lowPrice24h", Schema.STRING_SCHEMA)
            .field("highPriceToday", Schema.STRING_SCHEMA)
            .field("highPrice24h", Schema.STRING_SCHEMA)
            .field("todayOpeningPrice", Schema.STRING_SCHEMA)
            .build()
    }


    override fun version(): String {
        return KrakenSourceConnector().version()
    }

    override fun start(props: MutableMap<String, String>) {
        topic = props[KrakenSourceConnector.TOPIC_CONFIG]!!
        apiKey =  props[KrakenSourceConnector.API_KEY_CONFIG]!!
        apiSecret = props[KrakenSourceConnector.SECRET_CONFIG]!!
        currencyList = props[KrakenSourceConnector.CURRENCY_LIST_CONFIG]!!.split("|")
        pollingRate = props[KrakenSourceConnector.POLLING_RATE_CONFIG]!!.toLong()

        krakenHandler.setup(apiKey, apiSecret)
    }

    override fun stop() {
        krakenHandler.stop()
    }

    override fun poll(): MutableList<SourceRecord> {
        return if (Clock.systemDefaultZone().instant().toEpochMilli() - startTime.toEpochMilli() >= pollingRate) {
            val stockDataResponse: KrakenHandler.KrakenTickerResponse = krakenHandler.pullStockData(currencyList)
            val stockData: List<StockData> = convertResponseToStockData(stockDataResponse)
            val payload = stockData.map { convertStockDataToJson(it, krakenSchema) }
            startTime = Clock.systemDefaultZone().instant()

            mutableListOf(SourceRecord(
                buildSourcePartition(currencyList),
                buildSourceOffset(),
                topic,
                null,
                payloadSchema,
                payload))
        } else {
            mutableListOf()
        }
    }

    private fun buildSourcePartition(currencyList: List<String>): MutableMap<String, *>? {
        return mutableMapOf(Pair("currency", currencyList.joinToString(",")))
    }

    private fun buildSourceOffset(): MutableMap<String, *>? {
        return mutableMapOf(Pair("offset", offset++))
    }

    companion object {
        fun convertResponseToStockData(
            stockDataResponse: KrakenHandler.KrakenTickerResponse
        ): List<StockData> = stockDataResponse.result.map { currency ->
            val currencyValue = currency.value
            StockData(
                currency.key,
                ((currencyValue["a"] ?: error("Field [a] is not in the response")) as List<String>)[0],
                ((currencyValue["b"] ?: error("Field [b] is not in the response")) as List<String>)[0],
                ((currencyValue["c"] ?: error("Field [c] is not in the response")) as List<String>)[0],
                ((currencyValue["c"] ?: error("Field [c] is not in the response")) as List<String>)[1],
                ((currencyValue["t"] ?: error("Field [t] is not in the response")) as List<Int>)[0],
                ((currencyValue["t"] ?: error("Field [t] is not in the response")) as List<Int>)[1],
                ((currencyValue["l"] ?: error("Field [l] is not in the response")) as List<String>)[0],
                ((currencyValue["l"] ?: error("Field [l] is not in the response")) as List<String>)[1],
                ((currencyValue["h"] ?: error("Field [h] is not in the response")) as List<String>)[0],
                ((currencyValue["h"] ?: error("Field [h] is not in the response")) as List<String>)[1],
                ((currencyValue["o"] ?: error("Field [o] is not in the response")) as String)
            )
        }

        fun convertStockDataToJson(stockData: StockData, schema: Schema): Struct {
            val struct = Struct(schema)
            StockData::class.declaredMemberProperties.forEach { property ->
                struct.put(property.name, property.get(stockData))
            }

            return struct
        }
    }
}