package jh.couponevent.coupon.redis.api.dto

import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.Positive
import java.time.LocalDateTime

data class CreateCouponEventRequest(
    @field:NotBlank
    val name: String,

    @field:Positive
    val totalQuantity: Int,

    val startAt: LocalDateTime
)

data class CouponEventResponse(
    val id: Long,
    val name: String,
    val totalQuantity: Int,
    val issuedQuantity: Int,
    val startAt: LocalDateTime
)
