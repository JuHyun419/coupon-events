package jh.couponevent.coupon.redis.application

import jh.couponevent.coupon.domain.CouponEvent
import jh.couponevent.coupon.domain.CouponEventRepository
import jh.couponevent.coupon.exception.CouponEventNotFoundException
import jh.couponevent.coupon.redis.api.dto.CouponEventResponse
import jh.couponevent.coupon.redis.api.dto.CreateCouponEventRequest
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class CouponEventAdminServiceV2(
    private val couponEventRepository: CouponEventRepository,
    private val couponRedisIssuer: CouponRedisIssuer
) {
    @Transactional
    fun create(request: CreateCouponEventRequest): CouponEventResponse {
        val saved = couponEventRepository.save(
            CouponEvent(
                name = request.name,
                totalQuantity = request.totalQuantity,
                startAt = request.startAt
            )
        )
        val eventId = requireNotNull(saved.id)
        couponRedisIssuer.seedStock(eventId, request.totalQuantity)
        return saved.toResponse(issuedQuantity = 0)
    }

    fun findAll(name: String?): List<CouponEventResponse> {
        val events = if (name.isNullOrBlank()) {
            couponEventRepository.findAll()
        } else {
            couponEventRepository.findByNameContaining(name)
        }
        return events.map { it.toResponseWithRedisCount() }
    }

    fun findById(eventId: Long): CouponEventResponse {
        val event = couponEventRepository.findById(eventId)
            .orElseThrow { CouponEventNotFoundException(eventId) }
        return event.toResponseWithRedisCount()
    }

    private fun CouponEvent.toResponseWithRedisCount(): CouponEventResponse {
        val eventId = requireNotNull(id)
        val remaining = couponRedisIssuer.remainingStock(eventId)
        return toResponse(issuedQuantity = totalQuantity - remaining)
    }

    private fun CouponEvent.toResponse(issuedQuantity: Int) = CouponEventResponse(
        id = requireNotNull(id),
        name = name,
        totalQuantity = totalQuantity,
        issuedQuantity = issuedQuantity,
        startAt = startAt
    )
}
