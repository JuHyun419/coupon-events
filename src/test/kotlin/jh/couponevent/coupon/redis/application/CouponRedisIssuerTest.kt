package jh.couponevent.coupon.redis.application

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.core.StringRedisTemplate
import kotlin.test.assertEquals

class CouponRedisIssuerTest {

    private val connectionFactory = LettuceConnectionFactory("localhost", 6379).apply { afterPropertiesSet() }
    private val redisTemplate = StringRedisTemplate(connectionFactory).apply { afterPropertiesSet() }
    private val issuer = CouponRedisIssuer(redisTemplate)

    @BeforeEach
    fun setUp() {
        issuer.seedStock(TEST_EVENT_ID, 2)
    }

    @Test
    fun `재고가 있고 미발급 사용자면 SUCCESS를 반환하고 재고를 차감한다`() {
        val result = issuer.tryIssue(TEST_EVENT_ID, 1L)

        assertEquals(CouponIssueLuaResult.SUCCESS, result)
        assertEquals(1, issuer.remainingStock(TEST_EVENT_ID))
    }

    @Test
    fun `이미 발급받은 사용자가 재요청하면 DUPLICATE를 반환한다`() {
        issuer.tryIssue(TEST_EVENT_ID, 1L)

        val result = issuer.tryIssue(TEST_EVENT_ID, 1L)

        assertEquals(CouponIssueLuaResult.DUPLICATE, result)
    }

    @Test
    fun `재고가 소진되면 SOLD_OUT을 반환한다`() {
        issuer.tryIssue(TEST_EVENT_ID, 1L)
        issuer.tryIssue(TEST_EVENT_ID, 2L)

        val result = issuer.tryIssue(TEST_EVENT_ID, 3L)

        assertEquals(CouponIssueLuaResult.SOLD_OUT, result)
    }

    @Test
    fun `rollback하면 재고가 복구되고 발급자 목록에서 제거된다`() {
        issuer.tryIssue(TEST_EVENT_ID, 1L)

        issuer.rollback(TEST_EVENT_ID, 1L)

        assertEquals(2, issuer.remainingStock(TEST_EVENT_ID))
        assertEquals(CouponIssueLuaResult.SUCCESS, issuer.tryIssue(TEST_EVENT_ID, 1L))
    }

    companion object {
        private const val TEST_EVENT_ID = 999_999L
    }
}
