package jh.couponevent.coupon.kafka.application

import jh.couponevent.coupon.domain.CouponEventRepository
import jh.couponevent.coupon.exception.CouponEventNotFoundException
import jh.couponevent.coupon.exception.CouponEventNotOpenException
import jh.couponevent.coupon.exception.CouponIssuePublishFailedException
import jh.couponevent.coupon.exception.CouponSoldOutException
import jh.couponevent.coupon.exception.DuplicateCouponIssueException
import jh.couponevent.coupon.redis.application.CouponIssueLuaResult
import jh.couponevent.coupon.redis.application.CouponRedisIssuer
import org.springframework.stereotype.Service
import java.time.Clock
import java.time.LocalDateTime

data class CouponIssueAcceptedResult(
    val eventId: Long,
    val userId: Long,
    val issuedAt: LocalDateTime
)

@Service
class CouponIssueServiceV3(
    private val couponEventRepository: CouponEventRepository,
    private val couponRedisIssuer: CouponRedisIssuer,
    private val couponIssueEventPublisher: CouponIssueEventPublisher,
    private val clock: Clock
) {
    fun issue(eventId: Long, userId: Long): CouponIssueAcceptedResult {
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
            couponIssueEventPublisher.publish(eventId, userId, now)
        } catch (e: CouponIssuePublishFailedException) {
            couponRedisIssuer.rollback(eventId, userId)
            throw e
        }

        return CouponIssueAcceptedResult(eventId, userId, now)
    }
}
