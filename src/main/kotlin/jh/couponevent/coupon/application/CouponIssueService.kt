package jh.couponevent.coupon.application

import jh.couponevent.coupon.domain.IssuedCoupon
import jh.couponevent.coupon.domain.CouponEventRepository
import jh.couponevent.coupon.domain.IssuedCouponRepository
import jh.couponevent.coupon.exception.CouponEventNotFoundException
import jh.couponevent.coupon.exception.CouponEventNotOpenException
import jh.couponevent.coupon.exception.CouponSoldOutException
import jh.couponevent.coupon.exception.DuplicateCouponIssueException
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.Clock
import java.time.LocalDateTime

data class CouponIssueResult(
    val eventId: Long,
    val userId: Long,
    val issuedAt: LocalDateTime
)

@Service
class CouponIssueService(
    private val couponEventRepository: CouponEventRepository,
    private val issuedCouponRepository: IssuedCouponRepository,
    private val clock: Clock
) {
    @Transactional
    fun issue(eventId: Long, userId: Long): CouponIssueResult {
        val event = couponEventRepository.findByIdForUpdate(eventId)
            ?: throw CouponEventNotFoundException(eventId)

        val now = LocalDateTime.now(clock)
        if (now.isBefore(event.startAt)) {
            throw CouponEventNotOpenException(eventId)
        }

        if (event.issuedQuantity >= event.totalQuantity) {
            throw CouponSoldOutException(eventId)
        }

        if (issuedCouponRepository.existsByCouponEventIdAndUserId(eventId, userId)) {
            throw DuplicateCouponIssueException(eventId, userId)
        }

        issuedCouponRepository.save(IssuedCoupon(couponEventId = eventId, userId = userId, issuedAt = now))
        event.issuedQuantity += 1

        return CouponIssueResult(eventId, userId, now)
    }
}
