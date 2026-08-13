package jh.couponevent.coupon.kafka.application

import jh.couponevent.coupon.exception.CouponIssuePublishFailedException
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper
import java.time.LocalDateTime
import java.util.concurrent.TimeUnit

const val COUPON_ISSUE_TOPIC = "coupon-issue-events"

data class CouponIssueEvent(
    val eventId: Long,
    val userId: Long,
    val issuedAt: LocalDateTime,
)

@Component
class CouponIssueEventPublisher(
    private val kafkaTemplate: KafkaTemplate<String, String>,
) {
    private val objectMapper = ObjectMapper()

    fun publish(eventId: Long, userId: Long, issuedAt: LocalDateTime) {
        val payload = objectMapper.writeValueAsString(CouponIssueEvent(eventId, userId, issuedAt))
        try {
            kafkaTemplate.send(COUPON_ISSUE_TOPIC, userId.toString(), payload)
                .get(3, TimeUnit.SECONDS)
        } catch (e: Exception) {
            throw CouponIssuePublishFailedException(eventId, userId, cause = e)
        }
    }
}
