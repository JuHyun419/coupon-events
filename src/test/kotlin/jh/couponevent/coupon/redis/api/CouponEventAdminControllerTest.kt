package jh.couponevent.coupon.redis.api

import jh.couponevent.coupon.redis.api.dto.CouponEventResponse
import jh.couponevent.coupon.redis.api.dto.CreateCouponEventRequest
import jh.couponevent.coupon.redis.application.CouponEventAdminService
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.time.LocalDateTime
import kotlin.test.assertEquals

class CouponEventAdminControllerTest {

    private val couponEventAdminService: CouponEventAdminService = mock()
    private val controller = CouponEventAdminController(couponEventAdminService)

    @Test
    fun `이벤트 생성 요청을 서비스로 위임하고 201 응답을 만든다`() {
        val startAt = LocalDateTime.of(2026, 7, 11, 10, 0)
        val request = CreateCouponEventRequest(name = "여름 쿠폰 v2", totalQuantity = 10000, startAt = startAt)
        val response = CouponEventResponse(id = 1L, name = "여름 쿠폰 v2", totalQuantity = 10000, issuedQuantity = 0, startAt = startAt)
        whenever(couponEventAdminService.create(request)).thenReturn(response)

        val result = controller.create(request)

        assertEquals(201, result.statusCode.value())
        assertEquals(response, result.body)
    }

    @Test
    fun `목록 조회 요청을 서비스로 위임한다`() {
        whenever(couponEventAdminService.findAll(null)).thenReturn(emptyList())

        val result = controller.list(null)

        assertEquals(emptyList(), result)
    }
}
