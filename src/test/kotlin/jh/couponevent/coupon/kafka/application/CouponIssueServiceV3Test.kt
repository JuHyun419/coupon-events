package jh.couponevent.coupon.kafka.application

import jh.couponevent.coupon.domain.CouponEvent
import jh.couponevent.coupon.domain.CouponEventRepository
import jh.couponevent.coupon.exception.CouponEventNotFoundException
import jh.couponevent.coupon.exception.CouponEventNotOpenException
import jh.couponevent.coupon.exception.CouponIssuePublishFailedException
import jh.couponevent.coupon.exception.CouponSoldOutException
import jh.couponevent.coupon.exception.DuplicateCouponIssueException
import jh.couponevent.coupon.redis.application.CouponIssueLuaResult
import jh.couponevent.coupon.redis.application.CouponRedisIssuer
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Clock
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.Optional
import kotlin.test.assertEquals

class CouponIssueServiceV3Test {

    private val couponEventRepository: CouponEventRepository = mock()
    private val couponRedisIssuer: CouponRedisIssuer = mock()
    private val couponIssueEventPublisher: CouponIssueEventPublisher = mock()
    private val fixedClock: Clock = Clock.fixed(Instant.parse("2026-08-13T10:00:00Z"), ZoneOffset.UTC)
    private val service =
        CouponIssueServiceV3(couponEventRepository, couponRedisIssuer, couponIssueEventPublisher, fixedClock)

    private fun eventWithId(
        id: Long = 1L,
        startAt: LocalDateTime = LocalDateTime.of(2026, 8, 13, 9, 0)
    ): CouponEvent {
        val event = CouponEvent(name = "test-event", totalQuantity = 10, startAt = startAt)
        event.id = id
        return event
    }

    @Test
    fun `Redis가 SUCCESS를 반환하고 발행에 성공하면 발급이 접수된다`() {
        whenever(couponEventRepository.findById(1L)).thenReturn(Optional.of(eventWithId()))
        whenever(couponRedisIssuer.tryIssue(1L, 100L)).thenReturn(CouponIssueLuaResult.SUCCESS)

        val result = service.issue(1L, 100L)

        assertEquals(1L, result.eventId)
        assertEquals(100L, result.userId)
    }

    @Test
    fun `이벤트가 존재하지 않으면 CouponEventNotFoundException`() {
        whenever(couponEventRepository.findById(1L)).thenReturn(Optional.empty())

        assertThrows<CouponEventNotFoundException> { service.issue(1L, 100L) }
    }

    @Test
    fun `오픈 시각 이전이면 CouponEventNotOpenException`() {
        whenever(couponEventRepository.findById(1L))
            .thenReturn(Optional.of(eventWithId(startAt = LocalDateTime.of(2026, 8, 13, 11, 0))))

        assertThrows<CouponEventNotOpenException> { service.issue(1L, 100L) }
    }

    @Test
    fun `Redis가 SOLD_OUT을 반환하면 CouponSoldOutException`() {
        whenever(couponEventRepository.findById(1L)).thenReturn(Optional.of(eventWithId()))
        whenever(couponRedisIssuer.tryIssue(1L, 100L)).thenReturn(CouponIssueLuaResult.SOLD_OUT)

        assertThrows<CouponSoldOutException> { service.issue(1L, 100L) }
    }

    @Test
    fun `Redis가 DUPLICATE를 반환하면 DuplicateCouponIssueException`() {
        whenever(couponEventRepository.findById(1L)).thenReturn(Optional.of(eventWithId()))
        whenever(couponRedisIssuer.tryIssue(1L, 100L)).thenReturn(CouponIssueLuaResult.DUPLICATE)

        assertThrows<DuplicateCouponIssueException> { service.issue(1L, 100L) }
    }

    @Test
    fun `Kafka 발행이 실패하면 Redis를 롤백하고 예외를 다시 던진다`() {
        whenever(couponEventRepository.findById(1L)).thenReturn(Optional.of(eventWithId()))
        whenever(couponRedisIssuer.tryIssue(1L, 100L)).thenReturn(CouponIssueLuaResult.SUCCESS)
        whenever(couponIssueEventPublisher.publish(any(), any(), any()))
            .thenThrow(CouponIssuePublishFailedException(1L, 100L))

        assertThrows<CouponIssuePublishFailedException> { service.issue(1L, 100L) }

        verify(couponRedisIssuer).rollback(1L, 100L)
    }
}
