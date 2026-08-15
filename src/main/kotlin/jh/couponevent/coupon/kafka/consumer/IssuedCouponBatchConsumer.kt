package jh.couponevent.coupon.kafka.consumer

import jh.couponevent.coupon.domain.IssuedCoupon
import jh.couponevent.coupon.domain.IssuedCouponRepository
import jh.couponevent.coupon.kafka.application.COUPON_ISSUE_TOPIC
import jh.couponevent.coupon.kafka.application.CouponIssueEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper

@Component
class IssuedCouponBatchConsumer(
    private val issuedCouponRepository: IssuedCouponRepository
) {
    private val objectMapper = ObjectMapper()

    @KafkaListener(topics = [COUPON_ISSUE_TOPIC], groupId = "coupon-issue-consumer")
    fun consume(records: List<ConsumerRecord<String, String>>, ack: Acknowledgment) {
        val coupons = records.map { it.toIssuedCoupon() }
        try {
            issuedCouponRepository.saveAll(coupons)
        } catch (e: DataIntegrityViolationException) {
            records.forEach { record ->
                runCatching { issuedCouponRepository.save(record.toIssuedCoupon()) }
            }
        }
        ack.acknowledge()
    }

    private fun ConsumerRecord<String, String>.toIssuedCoupon(): IssuedCoupon {
        val event = objectMapper.readValue(value(), CouponIssueEvent::class.java)
        return IssuedCoupon(couponEventId = event.eventId, userId = event.userId, issuedAt = event.issuedAt)
    }
}
