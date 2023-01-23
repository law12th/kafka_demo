package utils

import config.KafkaConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import java.util.*

object StreamsUtils {
    fun createStreams(config: KafkaConfig, builder: StreamsBuilder): KafkaStreams {
        val streamsProperties = Properties()

        streamsProperties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServer)
        streamsProperties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, config.streamsApplicationId)
        streamsProperties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
        streamsProperties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)

        return KafkaStreams(builder.build(), streamsProperties)
    }

    fun startStreams(streams: KafkaStreams) {
        streams.start()
    }

}