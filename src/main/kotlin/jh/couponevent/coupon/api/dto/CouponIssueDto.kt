package jh.couponevent.coupon.api.dto

import jakarta.validation.constraints.Positive
import java.time.LocalDateTime

data class CouponIssueRequest(
    @field:Positive
    val userId: Long
)

data class CouponIssueResponse(
    val status: String,
    val issuedAt: LocalDateTime
)

data class CouponStatusResponse(
    val issued: Boolean
)
