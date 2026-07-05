package jh.couponevent.coupon.domain

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase
import org.springframework.dao.DataIntegrityViolationException
import java.time.LocalDateTime
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
class IssuedCouponRepositoryTest @Autowired constructor(
    private val couponEventRepository: CouponEventRepository,
    private val issuedCouponRepository: IssuedCouponRepository
) {

    @Test
    fun `동일한 이벤트-사용자 조합은 중복 저장할 수 없다`() {
        val event = couponEventRepository.save(
            CouponEvent(name = "unique-test", totalQuantity = 10, startAt = LocalDateTime.now())
        )
        issuedCouponRepository.saveAndFlush(
            IssuedCoupon(couponEventId = requireNotNull(event.id), userId = 1L)
        )

        assertThrows<DataIntegrityViolationException> {
            issuedCouponRepository.saveAndFlush(
                IssuedCoupon(couponEventId = requireNotNull(event.id), userId = 1L)
            )
        }
    }

    @Test
    fun `findByIdForUpdate는 존재하는 이벤트를 반환한다`() {
        val event = couponEventRepository.save(
            CouponEvent(name = "lock-test", totalQuantity = 10, startAt = LocalDateTime.now())
        )

        val found = couponEventRepository.findByIdForUpdate(requireNotNull(event.id))

        assertNotNull(found)
        assertEquals("lock-test", found?.name)
    }
}
