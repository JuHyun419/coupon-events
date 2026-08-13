package jh.couponevent.coupon.kafka.application

import jh.couponevent.coupon.domain.CouponEvent
import jh.couponevent.coupon.domain.CouponEventRepository
import jh.couponevent.coupon.domain.IssuedCouponRepository
import jh.couponevent.coupon.redis.application.CouponRedisIssuer
import org.awaitility.kotlin.await
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import java.time.Duration
import java.time.LocalDateTime
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals

@SpringBootTest
class CouponIssueConcurrencyV3Test @Autowired constructor(
    private val couponIssueService: CouponIssueServiceV3,
    private val couponEventRepository: CouponEventRepository,
    private val couponRedisIssuer: CouponRedisIssuer,
    private val issuedCouponRepository: IssuedCouponRepository
) {

    @Test
    fun `동시에 200명이 요청해도 Redis 재고 100개만 정확히 발급되고 결국 DB row 100개로 수렴한다`() {
        val totalQuantity = 100
        val requestCount = 200
        val event = couponEventRepository.save(
            CouponEvent(
                name = "kafka-concurrency-test-${System.currentTimeMillis()}",
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

        // Redis 차감은 동기적이므로 요청이 끝난 시점에 이미 확정되어 있다.
        assertEquals(totalQuantity, successCount.get())
        assertEquals(requestCount - totalQuantity, failCount.get())
        assertEquals(0, couponRedisIssuer.remainingStock(eventId))

        // DB 반영은 Kafka 컨슈머가 비동기로 처리하므로, 완료될 때까지 최대 30초 기다린다.
        await atMost Duration.ofSeconds(30) untilAsserted {
            assertEquals(totalQuantity.toLong(), issuedCouponRepository.countByCouponEventId(eventId))
        }

        val completedStatus = couponIssueService.status(eventId, 1L)
        assertEquals(CouponIssueStatus.COMPLETED, completedStatus.status)
    }
}
