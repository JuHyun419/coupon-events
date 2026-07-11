package jh.couponevent.coupon.redis.application

import jh.couponevent.coupon.domain.CouponEvent
import jh.couponevent.coupon.domain.CouponEventRepository
import jh.couponevent.coupon.domain.IssuedCouponRepository
import jh.couponevent.coupon.exception.CouponEventNotFoundException
import jh.couponevent.coupon.exception.CouponEventNotOpenException
import jh.couponevent.coupon.exception.CouponIssuePersistenceFailedException
import jh.couponevent.coupon.exception.CouponSoldOutException
import jh.couponevent.coupon.exception.DuplicateCouponIssueException
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

class CouponIssueServiceTest {

    private val couponEventRepository: CouponEventRepository = mock()
    private val issuedCouponRepository: IssuedCouponRepository = mock()
    private val couponRedisIssuer: CouponRedisIssuer = mock()
    private val fixedClock: Clock = Clock.fixed(Instant.parse("2026-07-11T10:00:00Z"), ZoneOffset.UTC)
    private val service = CouponIssueService(couponEventRepository, issuedCouponRepository, couponRedisIssuer, fixedClock)

    private fun eventWithId(
        id: Long = 1L,
        startAt: LocalDateTime = LocalDateTime.of(2026, 7, 11, 9, 0)
    ): CouponEvent {
        val event = CouponEvent(name = "test-event", totalQuantity = 10, startAt = startAt)
        event.id = id
        return event
    }

    @Test
    fun `Redis가 SUCCESS를 반환하면 발급에 성공한다`() {
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
            .thenReturn(Optional.of(eventWithId(startAt = LocalDateTime.of(2026, 7, 11, 11, 0))))

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
    fun `DB insert가 실패하면 Redis를 롤백하고 CouponIssuePersistenceFailedException을 던진다`() {
        whenever(couponEventRepository.findById(1L)).thenReturn(Optional.of(eventWithId()))
        whenever(couponRedisIssuer.tryIssue(1L, 100L)).thenReturn(CouponIssueLuaResult.SUCCESS)
        whenever(issuedCouponRepository.save(any())).thenThrow(RuntimeException("DB down"))

        assertThrows<CouponIssuePersistenceFailedException> { service.issue(1L, 100L) }

        verify(couponRedisIssuer).rollback(1L, 100L)
    }
}
