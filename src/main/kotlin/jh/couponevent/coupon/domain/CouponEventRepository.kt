package jh.couponevent.coupon.domain

import jakarta.persistence.LockModeType
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Lock
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param

interface CouponEventRepository : JpaRepository<CouponEvent, Long> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select c from CouponEvent c where c.id = :id")
    fun findByIdForUpdate(@Param("id") id: Long): CouponEvent?

    fun findByNameContaining(name: String): List<CouponEvent>
}
