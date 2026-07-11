package jh.couponevent.coupon.redis.application

import jh.couponevent.coupon.domain.CouponEventRepository
import jh.couponevent.coupon.domain.IssuedCoupon
import jh.couponevent.coupon.domain.IssuedCouponRepository
import jh.couponevent.coupon.exception.CouponEventNotFoundException
import jh.couponevent.coupon.exception.CouponEventNotOpenException
import jh.couponevent.coupon.exception.CouponIssuePersistenceFailedException
import jh.couponevent.coupon.exception.CouponSoldOutException
import jh.couponevent.coupon.exception.DuplicateCouponIssueException
import org.springframework.stereotype.Service
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
    private val couponRedisIssuer: CouponRedisIssuer,
    private val clock: Clock
) {
    fun issue(eventId: Long, userId: Long): CouponIssueResult {
        val event = couponEventRepository.findById(eventId)
            .orElseThrow { CouponEventNotFoundException(eventId) }

        val now = LocalDateTime.now(clock)
        if (now.isBefore(event.startAt)) {
            throw CouponEventNotOpenException(eventId)
        }

        when (couponRedisIssuer.tryIssue(eventId, userId)) {
            CouponIssueLuaResult.DUPLICATE -> throw DuplicateCouponIssueException(eventId, userId)
            CouponIssueLuaResult.SOLD_OUT -> throw CouponSoldOutException(eventId)
            CouponIssueLuaResult.SUCCESS -> Unit
        }

        try {
            issuedCouponRepository.save(IssuedCoupon(couponEventId = eventId, userId = userId, issuedAt = now))
        } catch (e: Exception) {
            couponRedisIssuer.rollback(eventId, userId)
            throw CouponIssuePersistenceFailedException(eventId, userId, cause = e)
        }

        return CouponIssueResult(eventId, userId, now)
    }
}
