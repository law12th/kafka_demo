import config.KafkaConfig
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import java.util.Properties

class AdminClient(
    config: KafkaConfig
) {
    private val adminClient: AdminClient

    init {
        val properties = Properties().apply {
            put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServer)
            put(AdminClientConfig.CLIENT_ID_CONFIG, config.adminClientId)
            put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000")
        }

        adminClient = AdminClient.create(properties)
    }

    fun createTopic(topic: String, numPartitions: Int, replicationFactor: Short) {
        val newTopic = NewTopic(topic, numPartitions, replicationFactor)

        adminClient.createTopics(listOf(newTopic))
    }

    fun topicExists(topic: String): Boolean {
      return adminClient.listTopics().names().get().contains(topic)
    }

    fun close() = adminClient.close()

}