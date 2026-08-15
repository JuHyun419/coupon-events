package jh.couponevent.coupon.kafka.api.dto

import jakarta.validation.constraints.Positive
import java.time.LocalDateTime

data class CouponIssueRequestV3(
    @field:Positive
    val userId: Long
)

data class CouponIssueAcceptedResponse(
    val status: String,
    val issuedAt: LocalDateTime
)
