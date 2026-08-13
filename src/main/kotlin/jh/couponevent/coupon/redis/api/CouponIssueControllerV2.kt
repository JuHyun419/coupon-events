package jh.couponevent.coupon.redis.api

import jakarta.validation.Valid
import jh.couponevent.coupon.domain.IssuedCouponRepository
import jh.couponevent.coupon.redis.api.dto.CouponIssueRequest
import jh.couponevent.coupon.redis.api.dto.CouponIssueResponse
import jh.couponevent.coupon.redis.api.dto.CouponStatusResponse
import jh.couponevent.coupon.redis.application.CouponIssueServiceV2
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/v2/coupon-events/{eventId}")
class CouponIssueControllerV2(
    private val couponIssueService: CouponIssueServiceV2,
    private val issuedCouponRepository: IssuedCouponRepository
) {
    @PostMapping("/issue")
    fun issue(@PathVariable eventId: Long, @Valid @RequestBody request: CouponIssueRequest): CouponIssueResponse {
        val result = couponIssueService.issue(eventId, request.userId)
        return CouponIssueResponse(status = "SUCCESS", issuedAt = result.issuedAt)
    }

    @GetMapping("/users/{userId}/status")
    fun status(@PathVariable eventId: Long, @PathVariable userId: Long): CouponStatusResponse {
        val issued = issuedCouponRepository.existsByCouponEventIdAndUserId(eventId, userId)
        return CouponStatusResponse(issued = issued)
    }
}
