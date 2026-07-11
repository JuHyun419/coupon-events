package jh.couponevent.common

import jh.couponevent.coupon.exception.CouponEventNotFoundException
import jh.couponevent.coupon.exception.CouponEventNotOpenException
import jh.couponevent.coupon.exception.CouponSoldOutException
import jh.couponevent.coupon.exception.DuplicateCouponIssueException
import jh.couponevent.coupon.exception.CouponIssuePersistenceFailedException
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice

data class ErrorResponse(val status: String, val message: String)

@RestControllerAdvice
class GlobalExceptionHandler {

    @ExceptionHandler(CouponEventNotFoundException::class)
    fun handleNotFound(e: CouponEventNotFoundException): ResponseEntity<ErrorResponse> =
        ResponseEntity.status(HttpStatus.NOT_FOUND).body(ErrorResponse("NOT_FOUND", e.message.orEmpty()))

    @ExceptionHandler(CouponEventNotOpenException::class)
    fun handleNotOpen(e: CouponEventNotOpenException): ResponseEntity<ErrorResponse> =
        ResponseEntity.status(HttpStatus.FORBIDDEN).body(ErrorResponse("NOT_OPEN_YET", e.message.orEmpty()))

    @ExceptionHandler(DuplicateCouponIssueException::class)
    fun handleDuplicate(e: DuplicateCouponIssueException): ResponseEntity<ErrorResponse> =
        ResponseEntity.status(HttpStatus.CONFLICT).body(ErrorResponse("DUPLICATE", e.message.orEmpty()))

    @ExceptionHandler(CouponSoldOutException::class)
    fun handleSoldOut(e: CouponSoldOutException): ResponseEntity<ErrorResponse> =
        ResponseEntity.status(HttpStatus.GONE).body(ErrorResponse("SOLD_OUT", e.message.orEmpty()))

    @ExceptionHandler(CouponIssuePersistenceFailedException::class)
    fun handlePersistenceFailure(e: CouponIssuePersistenceFailedException): ResponseEntity<ErrorResponse> =
        ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
            .body(ErrorResponse("ISSUE_PERSISTENCE_FAILED", e.message.orEmpty()))
}
