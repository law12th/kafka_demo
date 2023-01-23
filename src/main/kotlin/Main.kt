import config.KafkaConfig
import org.apache.kafka.streams.StreamsBuilder
import utils.AdminUtils
import utils.StreamsUtils

fun main() {
  val config = KafkaConfig()

  try {
    val adminClient = AdminUtils.adminClient(config)

    if(!AdminUtils.topicExists(config.topicOne, adminClient)) {
      AdminUtils.createTopic(config.topicOne, 1, 1, adminClient)
    }

    if(!AdminUtils.topicExists(config.topicTwo, adminClient)) {
      AdminUtils.createTopic(config.topicTwo, 1, 1, adminClient)
    }

   val builder = StreamsBuilder()

   val stream = builder.stream<String, String>(config.topicOne)

   // this is where you will perform your stream aggregation. Research on how to accomplish this. Here I am simply capitalizing the values of topic one and sending them to topic two
   val processedStreams = stream.mapValues { value -> value.uppercase() }

   // send values to topic two
   processedStreams.to(config.topicTwo)

   val streams = StreamsUtils.createStreams(config, builder)

   StreamsUtils.startStreams(streams)

   AdminUtils.close(adminClient)

  } catch (e: Exception) {
    println("Error occurred: ${e.message}")
  }

}