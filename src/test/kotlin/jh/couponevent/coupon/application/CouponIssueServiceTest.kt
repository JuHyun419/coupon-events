package jh.couponevent.coupon.application

import jh.couponevent.coupon.domain.CouponEvent
import jh.couponevent.coupon.domain.CouponEventRepository
import jh.couponevent.coupon.domain.IssuedCouponRepository
import jh.couponevent.coupon.exception.CouponEventNotFoundException
import jh.couponevent.coupon.exception.CouponEventNotOpenException
import jh.couponevent.coupon.exception.CouponSoldOutException
import jh.couponevent.coupon.exception.DuplicateCouponIssueException
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.time.Clock
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import kotlin.test.assertEquals

class CouponIssueServiceTest {

    private val couponEventRepository: CouponEventRepository = mock()
    private val issuedCouponRepository: IssuedCouponRepository = mock()
    private val fixedClock: Clock = Clock.fixed(Instant.parse("2026-07-05T10:00:00Z"), ZoneOffset.UTC)
    private val service = CouponIssueService(couponEventRepository, issuedCouponRepository, fixedClock)

    private fun eventWithId(
        id: Long = 1L,
        totalQuantity: Int = 10,
        issuedQuantity: Int = 0,
        startAt: LocalDateTime = LocalDateTime.of(2026, 7, 5, 9, 0)
    ): CouponEvent {
        val event = CouponEvent(
            name = "test-event",
            totalQuantity = totalQuantity,
            startAt = startAt,
            issuedQuantity = issuedQuantity
        )
        event.id = id
        return event
    }

    @Test
    fun `재고가 남아있고 미발급 사용자면 발급에 성공한다`() {
        whenever(couponEventRepository.findByIdForUpdate(1L)).thenReturn(eventWithId())
        whenever(issuedCouponRepository.existsByCouponEventIdAndUserId(1L, 100L)).thenReturn(false)

        val result = service.issue(1L, 100L)

        assertEquals(1L, result.eventId)
        assertEquals(100L, result.userId)
    }

    @Test
    fun `이벤트가 존재하지 않으면 CouponEventNotFoundException`() {
        whenever(couponEventRepository.findByIdForUpdate(1L)).thenReturn(null)

        assertThrows<CouponEventNotFoundException> { service.issue(1L, 100L) }
    }

    @Test
    fun `오픈 시각 이전이면 CouponEventNotOpenException`() {
        whenever(couponEventRepository.findByIdForUpdate(1L))
            .thenReturn(eventWithId(startAt = LocalDateTime.of(2026, 7, 5, 11, 0)))

        assertThrows<CouponEventNotOpenException> { service.issue(1L, 100L) }
    }

    @Test
    fun `재고가 소진되었으면 CouponSoldOutException`() {
        whenever(couponEventRepository.findByIdForUpdate(1L))
            .thenReturn(eventWithId(totalQuantity = 10, issuedQuantity = 10))

        assertThrows<CouponSoldOutException> { service.issue(1L, 100L) }
    }

    @Test
    fun `이미 발급받은 사용자면 DuplicateCouponIssueException`() {
        whenever(couponEventRepository.findByIdForUpdate(1L)).thenReturn(eventWithId())
        whenever(issuedCouponRepository.existsByCouponEventIdAndUserId(1L, 100L)).thenReturn(true)

        assertThrows<DuplicateCouponIssueException> { service.issue(1L, 100L) }
    }
}
