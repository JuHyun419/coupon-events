package jh.couponevent.coupon.redis.application

import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.script.DefaultRedisScript
import org.springframework.stereotype.Component

enum class CouponIssueLuaResult {
    SUCCESS, DUPLICATE, SOLD_OUT
}

@Component
class CouponRedisIssuer(
    private val redisTemplate: StringRedisTemplate
) {
    private val issueScript = DefaultRedisScript(ISSUE_SCRIPT, String::class.java)

    fun tryIssue(eventId: Long, userId: Long): CouponIssueLuaResult {
        val result = redisTemplate.execute(
            issueScript,
            listOf(stockKey(eventId), issuedUsersKey(eventId)),
            userId.toString()
        )
        return CouponIssueLuaResult.valueOf(requireNotNull(result))
    }

    fun rollback(eventId: Long, userId: Long) {
        redisTemplate.opsForValue().increment(stockKey(eventId))
        redisTemplate.opsForSet().remove(issuedUsersKey(eventId), userId.toString())
    }

    fun seedStock(eventId: Long, totalQuantity: Int) {
        redisTemplate.opsForValue().set(stockKey(eventId), totalQuantity.toString())
        redisTemplate.delete(issuedUsersKey(eventId))
    }

    fun remainingStock(eventId: Long): Int =
        redisTemplate.opsForValue().get(stockKey(eventId))?.toInt() ?: 0

    private fun stockKey(eventId: Long) = "coupon:$eventId:stock"

    private fun issuedUsersKey(eventId: Long) = "coupon:$eventId:issued_users"

    companion object {
        private const val ISSUE_SCRIPT = """
            if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then
                return 'DUPLICATE'
            end
            local stock = tonumber(redis.call('GET', KEYS[1]))
            if stock <= 0 then
                return 'SOLD_OUT'
            end
            redis.call('DECR', KEYS[1])
            redis.call('SADD', KEYS[2], ARGV[1])
            return 'SUCCESS'
        """
    }
}
