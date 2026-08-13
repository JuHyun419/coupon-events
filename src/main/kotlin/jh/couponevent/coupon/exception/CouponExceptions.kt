package jh.couponevent.coupon.exception

class CouponEventNotFoundException(eventId: Long) : RuntimeException("Coupon event not found: $eventId")

class CouponEventNotOpenException(eventId: Long) : RuntimeException("Coupon event not open yet: $eventId")

class CouponSoldOutException(eventId: Long) : RuntimeException("Coupon event sold out: $eventId")

class DuplicateCouponIssueException(eventId: Long, userId: Long) :
    RuntimeException("User $userId already issued a coupon for event $eventId")

class CouponIssuePersistenceFailedException(eventId: Long, userId: Long, cause: Throwable? = null) :
    RuntimeException(
        "Failed to persist issued coupon for event $eventId, user $userId after Redis stock was decremented",
        cause
    )

class CouponIssuePublishFailedException(eventId: Long, userId: Long, cause: Throwable? = null) :
    RuntimeException(
        "Failed to publish issued coupon event for event $eventId, user $userId after Redis stock was decremented",
        cause
    )
