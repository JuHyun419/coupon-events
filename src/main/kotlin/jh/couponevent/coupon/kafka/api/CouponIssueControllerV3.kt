package jh.couponevent.coupon.kafka.api

import jakarta.validation.Valid
import jh.couponevent.coupon.kafka.api.dto.CouponIssueAcceptedResponse
import jh.couponevent.coupon.kafka.api.dto.CouponIssueRequestV3
import jh.couponevent.coupon.kafka.application.CouponIssueServiceV3
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/v3/coupon-events/{eventId}")
class CouponIssueControllerV3(
    private val couponIssueService: CouponIssueServiceV3
) {
    @PostMapping("/issue")
    fun issue(
        @PathVariable eventId: Long,
        @Valid @RequestBody request: CouponIssueRequestV3
    ): CouponIssueAcceptedResponse {
        val result = couponIssueService.issue(eventId, request.userId)
        return CouponIssueAcceptedResponse(status = "SUCCESS", issuedAt = result.issuedAt)
    }
}
