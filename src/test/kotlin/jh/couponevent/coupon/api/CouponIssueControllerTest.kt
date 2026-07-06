package jh.couponevent.coupon.api

import tools.jackson.databind.ObjectMapper
import jh.couponevent.coupon.api.dto.CouponIssueRequest
import jh.couponevent.coupon.application.CouponIssueResult
import jh.couponevent.coupon.application.CouponIssueService
import jh.couponevent.coupon.domain.IssuedCouponRepository
import jh.couponevent.coupon.exception.CouponSoldOutException
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest
import org.springframework.http.MediaType
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.post
import java.time.LocalDateTime

@WebMvcTest(CouponIssueController::class)
class CouponIssueControllerTest @Autowired constructor(
    private val mockMvc: MockMvc
) {
    @MockitoBean
    private lateinit var couponIssueService: CouponIssueService

    @MockitoBean
    private lateinit var issuedCouponRepository: IssuedCouponRepository

    private val objectMapper = ObjectMapper()

    @Test
    fun `재고가 소진되면 410 GONE과 SOLD_OUT 상태를 반환한다`() {
        whenever(couponIssueService.issue(any(), any())).thenThrow(CouponSoldOutException(1L))

        mockMvc.post("/api/coupon-events/1/issue") {
            contentType = MediaType.APPLICATION_JSON
            content = objectMapper.writeValueAsString(CouponIssueRequest(userId = 100L))
        }.andExpect {
            status { isGone() }
            jsonPath("$.status") { value("SOLD_OUT") }
        }
    }

    @Test
    fun `발급에 성공하면 200과 SUCCESS 상태를 반환한다`() {
        val issuedAt = LocalDateTime.of(2026, 7, 5, 10, 0)
        whenever(couponIssueService.issue(any(), any()))
            .thenReturn(CouponIssueResult(eventId = 1L, userId = 100L, issuedAt = issuedAt))

        mockMvc.post("/api/coupon-events/1/issue") {
            contentType = MediaType.APPLICATION_JSON
            content = objectMapper.writeValueAsString(CouponIssueRequest(userId = 100L))
        }.andExpect {
            status { isOk() }
            jsonPath("$.status") { value("SUCCESS") }
        }
    }
}
