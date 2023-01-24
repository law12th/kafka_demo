import config.KafkaConfig
import org.apache.kafka.streams.StreamsBuilder

fun main() {
  val config = KafkaConfig()

  try {
      val adminClient = AdminClient(config)

      if (!adminClient.topicExists(config.topicOne)) {
          adminClient.createTopic(config.topicOne, 1, 1)
      }

      if (!adminClient.topicExists(config.topicTwo)) {
            adminClient.createTopic(config.topicTwo, 1, 1)
      }

      adminClient.close()

      val builder = StreamsBuilder()

      builder.stream<String, String>(config.topicOne)
          .mapValues { value -> value.uppercase() }
          .to(config.topicTwo)

     Streams(config, builder).start()

  } catch (e: Exception) {
    println("Error occurred: ${e.message}")
  }

}