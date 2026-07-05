package jh.couponevent.coupon.domain

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.LocalDateTime

@Entity
@Table(name = "coupon_event")
class CouponEvent(
    @Column(nullable = false)
    val name: String,

    @Column(name = "total_quantity", nullable = false)
    val totalQuantity: Int,

    @Column(name = "start_at", nullable = false)
    val startAt: LocalDateTime,

    @Column(name = "issued_quantity", nullable = false)
    var issuedQuantity: Int = 0,

    @Column(name = "created_at", nullable = false)
    val createdAt: LocalDateTime = LocalDateTime.now()
) {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    var id: Long? = null
}
