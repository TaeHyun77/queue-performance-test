package com.example.QueueTest.queue

import com.example.QueueTest.util.Loggable
import org.springframework.http.ResponseEntity
import org.springframework.http.server.reactive.ServerHttpResponse
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono
import java.time.Instant

@RequestMapping("/queue")
@RestController
class QueueController(
    private val queueService: QueueService
): Loggable {

    // 대기열 등록
    @PostMapping("/register")
    fun registerUser(
        @RequestParam("user_id") userId: String,
        @RequestParam(defaultValue = "reserve") queueType: String
    ): Mono<Long> {
        val now = Instant.now()
        val enterTimestamp = now.epochSecond * 1_000_000_000L + now.nano

        return queueService.registerUserToWaitQueue(userId, queueType, enterTimestamp)
    }

    // 대기열 or 참가열에서 사용자 존재 유무 확인
    @GetMapping("/isExist")
    fun isExistUserInQueue(
        @RequestParam("user_id") userId: String,
        @RequestParam(defaultValue = "reserve") queueType: String,
        @RequestParam("queueCategory") queueCategory: String
    ): Mono<Boolean> {
        return queueService.isExistUserInWaitOrAllow(userId, queueType, queueCategory)
    }

    // 대기열 or 참가열에서 사용자 순위 조회
    @GetMapping("/search/ranking")
    fun searchUserRanking(
        @RequestParam("user_id") userId: String,
        @RequestParam(defaultValue = "reserve") queueType: String,
        @RequestParam("queueCategory") queueCategory: String
    ): Mono<Long> {
        return queueService.searchUserRanking(userId, queueType, queueCategory)
    }

    // 대기열 or 참가열에서 사용자 제거
    @DeleteMapping("/cancel")
    fun cancelUser(
        @RequestParam("user_id") userId: String,
        @RequestParam(defaultValue = "reserve") queueType: String,
        @RequestParam("queueCategory") queueCategory: String
    ) {
        queueService.cancelWaitUser(userId, queueType, queueCategory)
    }

    // 새로고침 시 대기열 후순위 재등록
    @PostMapping("/reEnter")
    fun reEnterQueue(
        @RequestParam("user_id") userId: String,
        @RequestParam(defaultValue = "reserve") queueType: String
    ) {
        log.info { "새로고침 호출" }
        queueService.reEnterWaitQueue(userId, queueType)
    }

    // 대기열 상위 count명을 참가열 이동
    @PostMapping("/allow")
    fun allowUser(
        @RequestParam(defaultValue = "reserve") queueType: String,
        @RequestParam("count") count: Long
    ): Mono<Long> {
        return queueService.allowUser(queueType, count)
    }

    // 토큰 유효성 확인
    @GetMapping("/isValidateToken")
    fun isAccessTokenValid(
        @RequestParam("user_id") userId: String,
        @RequestParam(defaultValue = "reserve") queueType: String,
        @RequestParam("token") token: String
    ): Boolean {
        return queueService.isAccessTokenValid(userId, queueType, token)
    }

    // 쿠키 토큰 저장
    @GetMapping("/createCookie")
    fun sendCookie(
        @RequestParam("user_id") userId: String,
        @RequestParam(defaultValue = "reserve") queueType: String,
        response: ServerHttpResponse
    ): ResponseEntity<String> {
        return queueService.sendCookie(userId, queueType, response)
    }
}
