package jh.couponevent.common

import jh.couponevent.coupon.exception.CouponSoldOutException
import jh.couponevent.coupon.exception.DuplicateCouponIssueException
import org.junit.jupiter.api.Test
import org.springframework.http.HttpStatus
import kotlin.test.assertEquals

class GlobalExceptionHandlerTest {
    private val handler = GlobalExceptionHandler()

    @Test
    fun `품절 예외는 410 GONE, SOLD_OUT 상태로 매핑된다`() {
        val response = handler.handleSoldOut(CouponSoldOutException(1L))

        assertEquals(HttpStatus.GONE, response.statusCode)
        assertEquals("SOLD_OUT", response.body?.status)
    }

    @Test
    fun `중복 발급 예외는 409 CONFLICT, DUPLICATE 상태로 매핑된다`() {
        val response = handler.handleDuplicate(DuplicateCouponIssueException(1L, 100L))

        assertEquals(HttpStatus.CONFLICT, response.statusCode)
        assertEquals("DUPLICATE", response.body?.status)
    }
}
