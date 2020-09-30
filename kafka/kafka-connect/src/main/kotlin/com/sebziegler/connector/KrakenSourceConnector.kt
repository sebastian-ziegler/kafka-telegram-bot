package com.sebziegler.connector

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector

class KrakenSourceConnector: SourceConnector() {
    companion object {
        const val TOPIC_CONFIG = "topic"
        const val API_KEY_CONFIG = "api_key"
        const val SECRET_CONFIG = "secret"
        const val CURRENCY_LIST_CONFIG = "currency_list"
        const val POLLING_RATE_CONFIG = "poll_rate"
    }

    private val DEFAULTCURRENCYLIST: List<String> = listOf("XXBTZEUR", "XXBTZUSD")

    private val CONFIGDEF = ConfigDef()
        .define(TOPIC_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "The topic to publish data to")
        .define(API_KEY_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, "Kraken API Key")
        .define(SECRET_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, "Kraken API Secret")
        .define(CURRENCY_LIST_CONFIG, ConfigDef.Type.LIST, DEFAULTCURRENCYLIST, ConfigDef.Importance.LOW, "List with comma separated of currencies to pull. Default: [XXBTZEUR, XXBTZUSD]")
        .define(POLLING_RATE_CONFIG, ConfigDef.Type.LONG, ConfigDef.Importance.MEDIUM, "Polling rate for future calls in ms")

    private lateinit var topic: String
    private lateinit var apiKey: String
    private lateinit var apiSecret: String
    private lateinit var currencyList: String
    private lateinit var pollingRate: String

    override fun version(): String {
        return "Under Development"
    }

    override fun start(props: MutableMap<String, String>?) {
        val parsedConfig = AbstractConfig(CONFIGDEF, props)

        topic = parsedConfig.getString(TOPIC_CONFIG)
        apiKey = parsedConfig.getString(API_KEY_CONFIG)
        apiSecret = parsedConfig.getString(SECRET_CONFIG)
        currencyList = parsedConfig.getList(CURRENCY_LIST_CONFIG).joinToString("|")
        pollingRate = parsedConfig.getLong(POLLING_RATE_CONFIG).toString()
    }

    override fun taskClass(): Class<out Task> {
        return KrakenSourceTask::class.java
    }

    override fun taskConfigs(maxTasks: Int): MutableList<MutableMap<String, String>> {
        val configs: MutableList<MutableMap<String, String>> = mutableListOf()
        val config: MutableMap<String, String> = mutableMapOf()

        config[TOPIC_CONFIG] = topic
        config[API_KEY_CONFIG] = apiKey
        config[SECRET_CONFIG] = apiSecret
        config[CURRENCY_LIST_CONFIG] = currencyList
        config[POLLING_RATE_CONFIG] = pollingRate

        configs.add(config)

        return configs
    }

    override fun stop() {
        println("Stopping in progress")
    }

    override fun config(): ConfigDef {
        return CONFIGDEF
    }
}