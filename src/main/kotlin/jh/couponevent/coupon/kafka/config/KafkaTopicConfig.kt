package jh.couponevent.coupon.kafka.config

import jh.couponevent.coupon.kafka.application.COUPON_ISSUE_TOPIC
import org.apache.kafka.clients.admin.NewTopic
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.util.backoff.FixedBackOff

@Configuration
class KafkaTopicConfig {
    @Bean
    fun couponIssueEventsTopic(): NewTopic =
        TopicBuilder.name(COUPON_ISSUE_TOPIC)
            .partitions(3)
            .replicas(1)
            .build()

    @Bean
    fun kafkaDefaultErrorHandler(): DefaultErrorHandler =
        DefaultErrorHandler(FixedBackOff(1000L, 3))
}
