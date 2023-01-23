package utils

import config.KafkaConfig
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import java.util.Properties

object AdminUtils {
    fun adminClient(config: KafkaConfig): AdminClient {
      val adminProperties = Properties()

      adminProperties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServer)
      adminProperties.setProperty(AdminClientConfig.CLIENT_ID_CONFIG, config.adminClientId)
      adminProperties.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000")

      return AdminClient.create(adminProperties)
    }

    fun createTopic(topic: String, numPartitions: Int, replicationFactor: Short, adminClient: AdminClient) {
        val newTopic = NewTopic(topic, numPartitions, replicationFactor)

        adminClient.createTopics(listOf(newTopic))
    }

    fun topicExists(topic: String, adminClient: AdminClient): Boolean {
      return adminClient.listTopics().names().get().contains(topic)
    }

    fun close(adminClient: AdminClient) {
        adminClient.close()
    }
}