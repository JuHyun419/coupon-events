package jh.couponevent.coupon.kafka.api

import jh.couponevent.coupon.exception.CouponSoldOutException
import jh.couponevent.coupon.kafka.api.dto.CouponIssueRequestV3
import jh.couponevent.coupon.kafka.application.CouponIssueAcceptedResult
import jh.couponevent.coupon.kafka.application.CouponIssueServiceV3
import jh.couponevent.coupon.kafka.application.CouponIssueStatus
import jh.couponevent.coupon.kafka.application.CouponStatusResponseV3
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.whenever
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest
import org.springframework.http.MediaType
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.get
import org.springframework.test.web.servlet.post
import tools.jackson.databind.ObjectMapper
import java.time.LocalDateTime

@WebMvcTest(CouponIssueControllerV3::class)
class CouponIssueControllerV3Test @Autowired constructor(
    private val mockMvc: MockMvc
) {
    @MockitoBean
    private lateinit var couponIssueService: CouponIssueServiceV3

    private val objectMapper = ObjectMapper()

    @Test
    fun `재고가 소진되면 410 GONE과 SOLD_OUT 상태를 반환한다`() {
        whenever(couponIssueService.issue(any(), any())).thenThrow(CouponSoldOutException(1L))

        mockMvc.post("/api/v3/coupon-events/1/issue") {
            contentType = MediaType.APPLICATION_JSON
            content = objectMapper.writeValueAsString(CouponIssueRequestV3(userId = 100L))
        }.andExpect {
            status { isGone() }
            jsonPath("$.status") { value("SOLD_OUT") }
        }
    }

    @Test
    fun `발급이 접수되면 200과 SUCCESS 상태를 반환한다`() {
        val issuedAt = LocalDateTime.of(2026, 8, 13, 10, 0)
        whenever(couponIssueService.issue(any(), any()))
            .thenReturn(CouponIssueAcceptedResult(eventId = 1L, userId = 100L, issuedAt = issuedAt))

        mockMvc.post("/api/v3/coupon-events/1/issue") {
            contentType = MediaType.APPLICATION_JSON
            content = objectMapper.writeValueAsString(CouponIssueRequestV3(userId = 100L))
        }.andExpect {
            status { isOk() }
            jsonPath("$.status") { value("SUCCESS") }
        }
    }

    @Test
    fun `상태 조회는 서비스 결과를 그대로 응답한다`() {
        whenever(couponIssueService.status(1L, 100L)).thenReturn(
            CouponStatusResponseV3(eventId = 1L, userId = 100L, status = CouponIssueStatus.PENDING, issuedAt = null)
        )

        mockMvc.get("/api/v3/coupon-events/1/users/100/status").andExpect {
            status { isOk() }
            jsonPath("$.status") { value("PENDING") }
        }
    }
}
