package jh.couponevent.coupon.redis.api

import jakarta.validation.Valid
import jh.couponevent.coupon.redis.api.dto.CouponEventResponse
import jh.couponevent.coupon.redis.api.dto.CreateCouponEventRequest
import jh.couponevent.coupon.redis.application.CouponEventAdminService
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/v2/admin/coupon-events")
class CouponEventAdminController(
    private val couponEventAdminService: CouponEventAdminService
) {
    @PostMapping
    fun create(@Valid @RequestBody request: CreateCouponEventRequest): ResponseEntity<CouponEventResponse> =
        ResponseEntity.status(HttpStatus.CREATED).body(couponEventAdminService.create(request))

    @GetMapping
    fun list(@RequestParam(required = false) name: String?): List<CouponEventResponse> =
        couponEventAdminService.findAll(name)

    @GetMapping("/{eventId}")
    fun get(@PathVariable eventId: Long): CouponEventResponse =
        couponEventAdminService.findById(eventId)
}
