package com.sebziegler.kraken

import com.sebziegler.connector.KrakenSourceTask
import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.features.json.*
import io.ktor.client.request.*
import io.ktor.http.*
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable

object KrakenHandlerCompanion {
    @JvmStatic
    fun main(args: Array<String>) {
        val handler = KrakenHandler()
        val stockData = handler.pullStockData(listOf("XXBTZEUR"))
        println(stockData.result)
        println(KrakenSourceTask.convertResponseToStockData(stockData))
    }
}

class KrakenHandler {
    private lateinit var apiKey: String
    private lateinit var apiSecret: String
    private val httpClient = HttpClient(CIO) {
        install(JsonFeature) {
            serializer = GsonSerializer()
        }
    }

    fun setup(apiKey: String, apiSecret: String) {
        this.apiKey = apiKey
        this.apiSecret = apiSecret
    }

    @Serializable
    data class KrakenTickerResponse(
        val error: List<String>,
        val result: Map<String, Map<String, Any>>
    )

    fun pullStockData(currencyList: List<String>): KrakenTickerResponse {
        val currency = if (currencyList.isNotEmpty()) {
            currencyList.joinToString(",")
        } else {
            "XXBTZEUR"
        }
        val url = Url("https://api.kraken.com/0/public/Ticker?pair=$currency")
        return runBlocking {
            httpClient.get(url)
        }
    }

    fun stop() {
        httpClient.close()
    }
}