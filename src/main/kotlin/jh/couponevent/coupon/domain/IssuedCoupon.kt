package jh.couponevent.coupon.domain

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
import jakarta.persistence.UniqueConstraint
import java.time.LocalDateTime

@Entity
@Table(
    name = "issued_coupon",
    uniqueConstraints = [
        UniqueConstraint(name = "uk_event_user", columnNames = ["coupon_event_id", "user_id"])
    ]
)
class IssuedCoupon(
    @Column(name = "coupon_event_id", nullable = false)
    val couponEventId: Long,

    @Column(name = "user_id", nullable = false)
    val userId: Long,

    @Column(name = "issued_at", nullable = false)
    val issuedAt: LocalDateTime = LocalDateTime.now()
) {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    var id: Long? = null
}
