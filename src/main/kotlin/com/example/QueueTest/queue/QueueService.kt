package com.example.QueueTest.queue

import com.example.QueueTest.kafka.KafkaProducerService
import com.example.QueueTest.util.ACCESS_TOKEN
import com.example.QueueTest.util.ALLOW_QUEUE
import com.example.QueueTest.util.Loggable
import com.example.QueueTest.util.WAIT_QUEUE
import com.example.integrated.reserveException.ErrorCode
import com.example.integrated.reserveException.ReserveException
import org.springframework.data.domain.Range
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseCookie
import org.springframework.http.ResponseEntity
import org.springframework.http.server.reactive.ServerHttpResponse
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException
import java.time.Duration
import java.time.Instant

@Service
class QueueService (
    private val kafkaProducerService: KafkaProducerService,
    private val reactiveRedisTemplate: ReactiveRedisTemplate<String, String>
): Loggable {

    fun registerUserToWaitQueue(
        userId: String, queueType: String, enterTimestamp: Long
    ): Mono<Long> {
        val key = "$queueType$WAIT_QUEUE"

        val userExists = Mono.zip(
            isExistUserInWaitOrAllow(userId, queueType, "wait"),
            isExistUserInWaitOrAllow(userId, queueType, "allow")
        ) { existsInWait, existsInAllow -> existsInWait || existsInAllow }

        return userExists
            .flatMap { exists ->

                // 이미 존재하는 경우
                if (exists) {
                    Mono.error(ReserveException(HttpStatus.BAD_REQUEST, ErrorCode.ALREADY_REGISTERED_USER))

                // 존재하지 않는 경우
                } else {
                    reactiveRedisTemplate.opsForZSet()
                        .add(key, userId, enterTimestamp.toDouble())
                }
            }
            // add의 결과가 true일 때 진행
            .filter { it }
            .switchIfEmpty(Mono.error(ReserveException(HttpStatus.BAD_REQUEST, ErrorCode.ALREADY_REGISTERED_USER)))
            .flatMap {
                reactiveRedisTemplate.opsForZSet()
                    .rank(key, userId)
                    .switchIfEmpty(Mono.just(-1L))
                    .map { it + 1 }
                    .flatMap { rank ->
                        kafkaProducerService.sendMessage(queueType, userId).thenReturn(rank)
                    }
            }
            .doOnSuccess {
                log.info { "$userId 님 ${it}번째로 사용자 대기열 등록 성공" }
            }
    }


    fun isExistUserInWaitOrAllow(
        userId: String, queueType: String, queueCategory: String
    ): Mono<Boolean> {
        val keyType = if (queueCategory == "wait") WAIT_QUEUE else ALLOW_QUEUE
        val key = "$queueType$keyType"

        return reactiveRedisTemplate.opsForZSet()
            .rank(key, userId)

            // 스트림에 값이 하나라도 있으면 Mono<true>, 없으면 Mono<false>를 반환
            .hasElement()
            .doOnSuccess { exists ->
                log.info { "$userId 님 ${if (queueCategory == "wait") "대기열" else "참가열"} 존재 여부 : $exists" }
            }
    }

    fun searchUserRanking(
        userId: String, queueType: String, queueCategory: String
    ): Mono<Long> {
        val keyType = if (queueCategory == "wait") WAIT_QUEUE else ALLOW_QUEUE
        val key = "$queueType$keyType"

        return reactiveRedisTemplate.opsForZSet()
            .rank(key, userId)
            .switchIfEmpty(Mono.just(-1L))
            .map { rank ->
                if (rank != -1L) rank + 1 else -1L
            }
            .doOnSuccess { resultRank ->
                if (resultRank <= 0) {
                    log.warn { "[$queueCategory] $userId 님이 존재하지 않습니다. 순위: $resultRank" }
                } else {
                    log.info { "[$queueCategory] $userId 님의 현재 순위는 ${resultRank}번입니다." }
                }
            }
    }

    fun cancelWaitUser(
        userId: String, queueType: String, queueCategory: String
    ): Mono<Boolean> {
        val waitQueueKey = "$queueType$WAIT_QUEUE"
        val allowQueueKey = "$queueType$ALLOW_QUEUE"

        return if (queueCategory == "wait") {
            reactiveRedisTemplate.opsForZSet()
                .remove(waitQueueKey, userId)
                .flatMap { removedCount ->
                    if (removedCount > 0) {
                        kafkaProducerService.sendMessage(queueType, userId)
                            .thenReturn(true) // Mono로 반환
                    } else {
                        Mono.just(false)
                    }
                }
                .doOnSuccess { isCanceled ->
                    log.info { "$userId 님 대기열에서 취소 완료: $isCanceled" }
                }
        } else {
            reactiveRedisTemplate.opsForZSet()
                .remove(allowQueueKey, userId)
                .flatMap { removedCount ->
                    if (removedCount > 0) {
                        val tokenTtlKey = "token:$userId:TTL"

                        reactiveRedisTemplate.delete(tokenTtlKey)
                            .doOnSuccess {
                                log.info { "$userId 님의 TTL 키 삭제 완료" }
                            }
                            .doOnError { e ->
                                log.error(e) { "$userId 님의 TTL 키 삭제 중 오류 발생" }
                            }
                            .thenReturn(true)
                    } else {
                        Mono.just(false)
                    }
                }
                .doOnSuccess {
                    log.info { "$userId 님 참가열에서 취소 완료" }
                }
        }
    }

    fun generateAccessToken(
        userId: String, queueType: String
    ): String {
        return try {
            val digest = MessageDigest.getInstance("SHA-256")
            val raw = queueType + ACCESS_TOKEN + userId
            val hash = digest.digest(raw.toByteArray(StandardCharsets.UTF_8))

            hash.joinToString("") { "%02x".format(it) }
        } catch (e: NoSuchAlgorithmException) {
            throw RuntimeException("Token 생성 실패", e)
        }
    }

    fun sendCookie(
        userId: String, queueType: String, response: ServerHttpResponse
    ): ResponseEntity<String> {

        val encodedName = URLEncoder.encode(userId, StandardCharsets.UTF_8)
        val token = generateAccessToken(userId, queueType)
        val cookieName = queueType + "_user-access-cookie_" + "$encodedName"

        val responseCookie = ResponseCookie.from(cookieName, token)
            .path("/")
            .maxAge(Duration.ofSeconds(300))
            .build()

        response.addCookie(responseCookie)

        return ResponseEntity.ok("쿠키 발급 완료")
    }

    fun isAccessTokenValid(userId: String, queueType: String, token: String): Boolean {
        val generatedToken = generateAccessToken(userId, queueType)

        return generatedToken == token
    }

    fun reEnterWaitQueue(userId: String, queueType: String) {
        val newTimestamp: Long = Instant.now().toEpochMilli()

        reactiveRedisTemplate.opsForZSet()
            .add("$queueType$WAIT_QUEUE", userId, newTimestamp.toDouble())

        kafkaProducerService.sendMessage(queueType, userId)
    }

    fun allowUser(
        queueType: String, count: Long
    ): Mono<Long> {
        val waitQueueKey = "$queueType$WAIT_QUEUE"
        val allowQueueKey = "$queueType$ALLOW_QUEUE"
        val range = Range.closed(0L, count - 1)

        return reactiveRedisTemplate.opsForZSet()
            .range(waitQueueKey, range)
            .flatMap { userId ->
                val timestamp = Instant.now().toEpochMilli()
                val tokenKey = "token:$userId:TTL"

                log.info { "참가열 이동 사용자 : $userId" }

                // 참가열로 이동
                reactiveRedisTemplate.opsForZSet()
                    .add(allowQueueKey, userId, timestamp.toDouble())
                    .flatMap { added ->
                        if (!added) {
                            Mono.empty()
                        } else {
                            reactiveRedisTemplate.opsForValue()
                                .set(tokenKey, "allowed", Duration.ofMinutes(10))
                                .then(
                                    // 참가열로 이동하기에 대기열에서 삭제
                                    reactiveRedisTemplate.opsForZSet()
                                        .remove(waitQueueKey, userId)
                                )
                                .then(
                                    kafkaProducerService.sendMessage(queueType, userId)
                                )
                                .thenReturn(userId)
                        }
                    }
            }
            .count()
            .doOnSuccess { allowedCount ->
                log.info { "참가열로 이동된 사용자 수: $allowedCount" }
            }
    }

    @Scheduled(fixedDelay = 3000, initialDelay = 40000)
    fun moveUserToAllowQ() {
        val maxAllowedUsers = 3L
        val queueTypes = listOf("reserve")

        queueTypes.forEach { queueType ->
            allowUser(queueType, maxAllowedUsers)
                .doOnSuccess { movedCount ->
                    if (movedCount > 0) {
                        log.info { "$queueType 에서 $movedCount 명의 사용자가 참가열로 이동되었습니다." }
                    } else {
                        log.info { "참가열로 이동된 사용자가 없습니다" }
                    }
                }
                .doOnError { e ->
                    log.error(e) { "allowUser 실행 실패: $queueType" }
                }
                .subscribe()
        }
    }
}