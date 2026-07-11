package jh.couponevent.coupon.application

import jh.couponevent.coupon.api.dto.CouponEventResponse
import jh.couponevent.coupon.api.dto.CreateCouponEventRequest
import jh.couponevent.coupon.domain.CouponEvent
import jh.couponevent.coupon.domain.CouponEventRepository
import jh.couponevent.coupon.exception.CouponEventNotFoundException
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class CouponEventAdminService(
    private val couponEventRepository: CouponEventRepository
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
        return saved.toResponse()
    }

    @Transactional(readOnly = true)
    fun findAll(name: String?): List<CouponEventResponse> {
        val events = if (name.isNullOrBlank()) {
            couponEventRepository.findAll()
        } else {
            couponEventRepository.findByNameContaining(name)
        }
        return events.map { it.toResponse() }
    }

    @Transactional(readOnly = true)
    fun findById(eventId: Long): CouponEventResponse {
        val event = couponEventRepository.findById(eventId)
            .orElseThrow { CouponEventNotFoundException(eventId) }
        return event.toResponse()
    }
}

private fun CouponEvent.toResponse() = CouponEventResponse(
    id = requireNotNull(id),
    name = name,
    totalQuantity = totalQuantity,
    issuedQuantity = issuedQuantity,
    startAt = startAt
)
