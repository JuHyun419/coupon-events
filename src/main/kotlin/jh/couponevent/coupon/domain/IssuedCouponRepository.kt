package jh.couponevent.coupon.domain

import org.springframework.data.jpa.repository.JpaRepository

interface IssuedCouponRepository : JpaRepository<IssuedCoupon, Long> {
    fun existsByCouponEventIdAndUserId(couponEventId: Long, userId: Long): Boolean
    fun countByCouponEventId(couponEventId: Long): Long
    fun findByCouponEventIdAndUserId(couponEventId: Long, userId: Long): IssuedCoupon?
}
