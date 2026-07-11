package jh.couponevent.coupon.redis.application

import jh.couponevent.coupon.domain.CouponEvent
import jh.couponevent.coupon.domain.CouponEventRepository
import jh.couponevent.coupon.domain.IssuedCouponRepository
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import java.time.LocalDateTime
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals

@SpringBootTest
class CouponIssueConcurrencyTest @Autowired constructor(
    private val couponIssueService: CouponIssueServiceV2,
    private val couponEventRepository: CouponEventRepository,
    private val couponRedisIssuer: CouponRedisIssuer,
    private val issuedCouponRepository: IssuedCouponRepository
) {

    @Test
    fun `동시에 200명이 요청해도 Redis 재고 100개만 정확히 발급된다`() {
        val totalQuantity = 100
        val requestCount = 200
        val event = couponEventRepository.save(
            CouponEvent(
                name = "redis-concurrency-test-${System.currentTimeMillis()}",
                totalQuantity = totalQuantity,
                startAt = LocalDateTime.now().minusMinutes(1)
            )
        )
        val eventId = requireNotNull(event.id)
        couponRedisIssuer.seedStock(eventId, totalQuantity)

        val executor = Executors.newFixedThreadPool(32)
        val latch = CountDownLatch(requestCount)
        val successCount = AtomicInteger(0)
        val failCount = AtomicInteger(0)

        (1..requestCount).forEach { userId ->
            executor.submit {
                try {
                    couponIssueService.issue(eventId, userId.toLong())
                    successCount.incrementAndGet()
                } catch (e: Exception) {
                    failCount.incrementAndGet()
                } finally {
                    latch.countDown()
                }
            }
        }
        latch.await(30, TimeUnit.SECONDS)
        executor.shutdown()

        assertEquals(totalQuantity, successCount.get())
        assertEquals(requestCount - totalQuantity, failCount.get())
        assertEquals(totalQuantity.toLong(), issuedCouponRepository.countByCouponEventId(eventId))
        assertEquals(0, couponRedisIssuer.remainingStock(eventId))
    }
}
