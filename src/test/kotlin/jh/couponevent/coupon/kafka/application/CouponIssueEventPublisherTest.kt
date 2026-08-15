package jh.couponevent.coupon.kafka.application

import jh.couponevent.coupon.exception.CouponIssuePublishFailedException
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.springframework.kafka.KafkaException
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import java.time.LocalDateTime
import java.util.concurrent.CompletableFuture

class CouponIssueEventPublisherTest {

    private val kafkaTemplate: KafkaTemplate<String, String> = mock()
    private val publisher = CouponIssueEventPublisher(kafkaTemplate)

    @Test
    fun `발행에 성공하면 예외 없이 반환한다`() {
        val issuedAt = LocalDateTime.of(2026, 8, 13, 10, 0)
        val sendResult: CompletableFuture<SendResult<String, String>> =
            CompletableFuture.completedFuture(mock())
        whenever(kafkaTemplate.send(eq(COUPON_ISSUE_TOPIC), eq("100"), any())).thenReturn(sendResult)

        publisher.publish(eventId = 1L, userId = 100L, issuedAt = issuedAt)
    }

    @Test
    fun `발행이 실패하면 CouponIssuePublishFailedException을 던진다`() {
        val issuedAt = LocalDateTime.of(2026, 8, 13, 10, 0)
        val failedFuture = CompletableFuture<SendResult<String, String>>()
        failedFuture.completeExceptionally(KafkaException("broker unreachable"))
        whenever(kafkaTemplate.send(eq(COUPON_ISSUE_TOPIC), eq("100"), any())).thenReturn(failedFuture)

        assertThrows<CouponIssuePublishFailedException> {
            publisher.publish(eventId = 1L, userId = 100L, issuedAt = issuedAt)
        }
    }
}
