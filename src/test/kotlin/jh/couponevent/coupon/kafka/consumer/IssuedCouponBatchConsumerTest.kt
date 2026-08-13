package jh.couponevent.coupon.kafka.consumer

import jh.couponevent.coupon.domain.IssuedCoupon
import jh.couponevent.coupon.domain.IssuedCouponRepository
import jh.couponevent.coupon.kafka.application.COUPON_ISSUE_TOPIC
import jh.couponevent.coupon.kafka.application.CouponIssueEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.argThat
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.kafka.support.Acknowledgment
import tools.jackson.databind.ObjectMapper
import java.time.LocalDateTime
import kotlin.test.assertEquals

class IssuedCouponBatchConsumerTest {

    private val issuedCouponRepository: IssuedCouponRepository = mock()
    private val objectMapper = ObjectMapper()
    private val consumer = IssuedCouponBatchConsumer(issuedCouponRepository)
    private val ack: Acknowledgment = mock()

    private fun record(eventId: Long, userId: Long, issuedAt: LocalDateTime): ConsumerRecord<String, String> =
        ConsumerRecord(
            COUPON_ISSUE_TOPIC, 0, 0L, userId.toString(),
            objectMapper.writeValueAsString(CouponIssueEvent(eventId, userId, issuedAt))
        )

    @Test
    fun `배치로 받은 레코드를 saveAll로 한 번에 저장하고 ack한다`() {
        val issuedAt = LocalDateTime.of(2026, 8, 13, 10, 0)
        val records = listOf(record(1L, 100L, issuedAt), record(1L, 101L, issuedAt))

        consumer.consume(records, ack)

        val captor = argumentCaptor<List<IssuedCoupon>>()
        verify(issuedCouponRepository).saveAll(captor.capture())
        val saved = captor.firstValue
        assertEquals(2, saved.size)
        assertEquals(setOf(100L, 101L), saved.map { it.userId }.toSet())
        verify(ack).acknowledge()
    }

    @Test
    fun `saveAll이 UNIQUE 제약 위반으로 실패하면 1건씩 재시도해 나머지는 반영하고 ack한다`() {
        val issuedAt = LocalDateTime.of(2026, 8, 13, 10, 0)
        val records = listOf(record(1L, 100L, issuedAt), record(1L, 101L, issuedAt))
        whenever(issuedCouponRepository.saveAll(any<List<IssuedCoupon>>()))
            .thenThrow(DataIntegrityViolationException("duplicate"))
        whenever(issuedCouponRepository.save(argThat<IssuedCoupon> { userId == 100L }))
            .thenThrow(DataIntegrityViolationException("duplicate"))
        whenever(issuedCouponRepository.save(argThat<IssuedCoupon> { userId == 101L }))
            .thenAnswer { invocation -> invocation.arguments[0] }

        consumer.consume(records, ack)

        verify(issuedCouponRepository).save(argThat<IssuedCoupon> { userId == 101L })
        verify(ack).acknowledge()
    }
}
