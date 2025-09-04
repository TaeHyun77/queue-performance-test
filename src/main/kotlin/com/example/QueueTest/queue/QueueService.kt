package com.example.QueueTest.queue

import com.example.QueueTest.idempotency.IdempotencyService
import com.example.QueueTest.kafka.KafkaProducerService
import com.example.QueueTest.util.ACCESS_TOKEN
import com.example.QueueTest.util.ALLOW_QUEUE
import com.example.QueueTest.util.Loggable
import com.example.QueueTest.util.TOKEN_TTL_INFO
import com.example.QueueTest.util.WAIT_QUEUE
import com.example.integrated.reserveException.ErrorCode
import com.example.integrated.reserveException.ReserveException
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseCookie
import org.springframework.http.ResponseEntity
import org.springframework.http.server.reactive.ServerHttpResponse
import org.springframework.stereotype.Service
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException
import java.time.Duration
import java.time.Instant

@Service
class QueueService (
    private val kafkaProducerService: KafkaProducerService,
    private val reactiveRedisTemplate: ReactiveRedisTemplate<String, String>,
    private val idempotencyService: IdempotencyService
): Loggable {

    suspend fun register(
        userId: String,
        queueType: String,
        enterTimestamp: Long,
        idempotencyKey: String
    ): ResponseEntity<String> {

        return idempotencyService.execute(
            key = idempotencyKey,
            url = "/queue/register",
            method = "POST",
        ) { registerUserToWaitQueue(userId, queueType, enterTimestamp) }
    }

    suspend fun registerUserToWaitQueue(
        userId: String,
        queueType: String,
        enterTimestamp: Long
    ): String {

        // 두 비동기 작업이 모두 완료된 후에야 다음 로직이 실행됨
        coroutineScope {
            val inWaitDeferred = async { searchUserRanking(userId, queueType, "wait") }
            val inAllowDeferred = async { searchUserRanking(userId, queueType, "allow") }

            // await() : 해당 async 작업의 결과가 나올 때까지 현재 코루틴을 일시 중단
            val inWait = inWaitDeferred.await()
            val inAllow = inAllowDeferred.await()

            if (inWait != -1L || inAllow != -1L) {
                throw ReserveException(HttpStatus.BAD_REQUEST, ErrorCode.ALREADY_REGISTERED_USER)
            }
        }

        val waitQueueKey = queueType + WAIT_QUEUE

        val wasAdded = reactiveRedisTemplate.opsForZSet()
            .add(waitQueueKey, userId, enterTimestamp.toDouble())
            .awaitSingle() // true : add 성공 , false : add 실패 -> 이미 존재

        if (!wasAdded) {
            throw ReserveException(HttpStatus.BAD_REQUEST, ErrorCode.ALREADY_REGISTERED_USER)
        } else {
            // 대기열에 성공적으로 추가 되었다면 카프카 메세지 전송
            kafkaProducerService.sendMessage(queueType)
        }

        val rank = searchUserRanking(userId, queueType, "wait")
        log.info{"${userId}님 ${rank}번째로 사용자 대기열 등록 성공" }

        return "REGISTERED"
    }

    /**
     * 대기열 or 참가열에서 사용자 순위 조회
     * 존재하지 않는다면 -1L 반환, 존재한다면 사용자의 순위 반환
     * */
    suspend fun searchUserRanking(
        userId: String,
        queueType: String,
        queueCategory: String
    ): Long {

        val keyType = if (queueCategory == "wait") WAIT_QUEUE else ALLOW_QUEUE

        val queueKey = queueType + keyType

        val redisRank = reactiveRedisTemplate.opsForZSet()
            .rank(queueKey, userId)
            .awaitSingleOrNull() // rank 값이 존재 : Long 값 반환 , rank 값이 존재하지 않음 : null 반환

        val rank = redisRank?.takeIf { it >= 0 }?.plus(1) ?: -1

        if (rank == -1L) {
            log.info { "[$queueCategory] $userId 님이 존재하지 않습니다." }
        } else {
            log.info { "[$queueCategory] $userId 님의 현재 순위 $rank" }
        }

        return rank
    }

    suspend fun cancelUser(userId: String, queueType: String, queueCategory: String): ResponseEntity<String> {
        return when (queueCategory) {
            "wait" -> cancelWaitOrAllow(userId, queueType)
            "allow" -> cancelAllowUser(userId, queueType)
            else -> throw ReserveException(HttpStatus.BAD_REQUEST, ErrorCode.INVALID_QUEUE_CATEGORY)
        }
    }

    /*
    * 문제 상황 발생 가능성
    *
    * 승격 로직이 wait에서 사용자를 삭제하고 allow로 옮기기 전에 취소 로직이 allow에서 삭제를 진행하는 경우
    * ⇒ 이러한 타이밍으로 인한 경쟁 상태를 별도로 관리하여 문제를 해결
    * */
    private suspend fun cancelWaitOrAllow(
        userId: String, queueType: String
    ): ResponseEntity<String> {
        val waitQueueKey = "$queueType$WAIT_QUEUE"

        val removedCount = reactiveRedisTemplate.opsForZSet()
            .remove(waitQueueKey, userId)
            .awaitSingle() // 0 : 해당 사용자 없음 , 1 : 해당 사용자 삭제

        return if (removedCount == 1L) {
            kafkaProducerService.sendMessage(queueType)
            ResponseEntity.ok("대기열 삭제 완료")
        } else {
            val allowResult = cancelAllowUserForWaitContext(userId, queueType)

            allowResult ?: run {
                log.info { "이미 삭제가 처리된 사용자" }
                ResponseEntity.ok("이미 삭제가 처리된 사용자입니다.")
            }
        }
    }

    // 경쟁 상태 문제로 인한 삭제 보완 로직
    private suspend fun cancelAllowUserForWaitContext(
        userId: String, queueType: String
    ): ResponseEntity<String>? {
        val allowQueueKey = "$queueType$ALLOW_QUEUE"

        val allowRemovedCount = reactiveRedisTemplate.opsForZSet()
            .remove(allowQueueKey, userId)
            .awaitSingle()

        // 승격 중 타이밍 문제로 간주
        if (allowRemovedCount == 0L) return null

        removeTtlKey(userId)
        return ResponseEntity.ok("참가열 삭제 완료")
    }

    suspend fun cancelAllowUser(
        userId: String,
        queueType: String,
    ): ResponseEntity<String> {

        try {
            val allowQueueKey = "$queueType$ALLOW_QUEUE"

            val allowRemovedCount = reactiveRedisTemplate.opsForZSet()
                .remove(allowQueueKey, userId)
                .awaitSingle()

            if (allowRemovedCount == 0L) {
                throw ReserveException(HttpStatus.BAD_REQUEST, ErrorCode.USER_NOT_FOUND_IN_THE_QUEUE)
            } else {
                removeTtlKey(userId)
                return ResponseEntity.ok("참가열 삭제 완료")
            }

        } catch (e: ReserveException) {
            log.error { "예약 취소 중 오류 발생" }
            throw e
        }
    }

    // TTL 키 삭제 로직
    private suspend fun removeTtlKey(userId: String) {

        val ttlRemovedCount = reactiveRedisTemplate.opsForZSet()
            .remove(TOKEN_TTL_INFO, userId)
            .awaitSingle()

        if (ttlRemovedCount == 0L) log.warn { "$userId TTL 키가 존재하지 않아 삭제되지 않았습니다." }
    }

    fun generateAccessToken(
        userId: String,
        queueType: String
    ): String {

        return try {
            val digest = MessageDigest.getInstance("SHA-256")
            val raw = queueType + ACCESS_TOKEN + userId
            val hash = digest.digest(raw.toByteArray(StandardCharsets.UTF_8))

            buildString {
                hash.forEach { append(String.format("%02x", it)) }
            }
        } catch (e: NoSuchAlgorithmException) {
            throw RuntimeException("Token 생성 실패", e)
        }
    }

    fun sendCookie(
        userId: String,
        queueType: String,
        response: ServerHttpResponse
    ): ResponseEntity<String> {

        val encodedName = URLEncoder.encode(userId, StandardCharsets.UTF_8)
        val token = generateAccessToken(userId, queueType)
        val cookieName = "reserve_user-access-cookie_$encodedName"

        val responseCookie = ResponseCookie.from(cookieName, token)
            .path("/")
            .maxAge(Duration.ofSeconds(300))
            .build()

        response.addCookie(responseCookie)

        return ResponseEntity.ok("쿠키 발급 완료")
    }

    suspend fun isAccessTokenValid(
        userId: String,
        queueType: String,
        token: String
    ): Boolean {

        // 해당 사용자의 TTL 값 조회
        // true : 사용자의 TTL 값 반환 , null : 값이 존재하지 않음
        val ttlInfo = reactiveRedisTemplate
            .opsForZSet()
            .score(TOKEN_TTL_INFO, userId)
            .awaitSingleOrNull() ?: throw ReserveException(HttpStatus.BAD_REQUEST, ErrorCode.NOT_EXIST_TTL_INFO);

        val expireTime = ttlInfo.toLong()
        val now = Instant.now().epochSecond

        return generateAccessToken(userId, queueType) == token && now <= expireTime
    }

//    suspend fun reEnterWaitQueue(
//        userId: String,
//        queueType: String
//    ) {
//
//        val now = Instant.now()
//        val newTimestamp = now.epochSecond * 1_000_000_000L + now.nano
//        val waitQueueKey = "$queueType$WAIT_QUEUE"
//
//        val added = reactiveRedisTemplate
//            .opsForZSet()
//            .add(waitQueueKey, userId, newTimestamp.toDouble())
//            .awaitSingle()
//
//        if (added == true) {
//            kafkaProducerService.sendMessage(queueType)
//        }
//    }

    suspend fun allowUser(
        queueType: String,
        count: Long
    ): Long {
        val waitQueueKey = "$queueType$WAIT_QUEUE"
        val allowQueueKey = "$queueType$ALLOW_QUEUE"

        val poppedUsers = reactiveRedisTemplate
            .opsForZSet()
            .popMin(waitQueueKey, count)
            .collectList()
            .awaitSingle() // 값이 있으면 List<T> 반환, 값이 없어도 빈 List 반환

        if (poppedUsers.isEmpty()) return 0

        val now = Instant.now()
        val timestamp = now.epochSecond * 1_000_000_000L + now.nano
        val expireAt = now.plus(Duration.ofMinutes(10)).epochSecond.toDouble()

        var movedCount = 0L

        poppedUsers.forEach { user ->
            val userId = user.value.toString()

            // 참가열에 사용자 이동
            reactiveRedisTemplate.opsForZSet()
                .add(allowQueueKey, userId, timestamp.toDouble())
                .awaitSingle()

            // 참가열 TTL 생성
            reactiveRedisTemplate.opsForZSet()
                .add(TOKEN_TTL_INFO, userId, expireAt)
                .awaitSingle()

            kafkaProducerService.sendMessage(queueType)
            movedCount++
        }

        return movedCount
    }
}