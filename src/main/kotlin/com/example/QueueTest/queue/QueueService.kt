package com.example.QueueTest.queue

import com.example.QueueTest.kafka.KafkaProducerService
import com.example.QueueTest.user.User
import com.example.QueueTest.user.UserRepository
import com.example.QueueTest.util.ACCESS_TOKEN
import com.example.QueueTest.util.ALLOW_QUEUE
import com.example.QueueTest.util.Loggable
import com.example.QueueTest.util.WAIT_QUEUE
import com.example.integrated.reserveException.ErrorCode
import com.example.integrated.reserveException.ReserveException
import jakarta.servlet.http.Cookie
import jakarta.servlet.http.HttpServletResponse
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException
import java.time.Duration
import java.time.Instant

@Service
class QueueService (
    private val kafkaProducerService: KafkaProducerService,
    private val redisTemplate: RedisTemplate<String, String>,
    private val userRepository: UserRepository
): Loggable {

    fun registerUserToWaitQueue(userId: String, queueType: String, enterTimestamp: Long): Long {
        // 대기열 및 참가열 사용자 존재 여부
        val existsInWaitQueue = isExistUserInWaitOrAllow(userId, queueType, "wait")
        val existsInAllowQueue = isExistUserInWaitOrAllow(userId, queueType, "allow")

        if (existsInWaitQueue || existsInAllowQueue) {
            throw ReserveException(HttpStatus.BAD_REQUEST, ErrorCode.ALREADY_REGISTERED_USER)
        }

        val added = redisTemplate.opsForZSet()
            .add("$queueType$WAIT_QUEUE", userId, enterTimestamp.toDouble())

        if (added == false) {
            throw ReserveException(HttpStatus.BAD_REQUEST, ErrorCode.ALREADY_REGISTERED_USER)
        }

        val rank = redisTemplate.opsForZSet()
            .rank("$queueType$WAIT_QUEUE", userId)

        val result = rank?.plus(1) ?: -1L

        changeUserStatus(queueType, userId, "wait")
        kafkaProducerService.sendMessage(queueType, userId)

        log.info { "$userId 님 ${result}번째로 사용자 대기열 등록 성공" }
        return result
    }

    fun isExistUserInWaitOrAllow(userId: String, queueType: String, queueCategory: String): Boolean {
        val keyType = if (queueCategory == "wait") WAIT_QUEUE else ALLOW_QUEUE

        val rank = redisTemplate.opsForZSet()
            .rank("$queueType$keyType", userId)

        val exists = rank != null && rank >= 0

        log.info { "$userId 님 ${if (queueCategory == "wait") "대기열" else "참가열"} 존재 여부 : $exists" }
        return exists
    }

    fun searchUserRanking(userId: String, queueType: String, queueCategory: String): Long {
        val keyType = if (queueCategory == "wait") WAIT_QUEUE else ALLOW_QUEUE

        val rank = redisTemplate.opsForZSet()
            .rank("$queueType$keyType", userId)

        val resultRank = rank?.plus(1) ?: -1L

        if (resultRank <= 0) {
            log.warn { "[$queueCategory] $userId 님이 존재하지 않습니다. 순위: $resultRank" }
        } else {
            log.info { "[$queueCategory] $userId 님의 현재 순위는 ${resultRank}번입니다." }
        }
        return resultRank
    }

    fun cancelWaitUser(userId: String, queueType: String, queueCategory: String) {
        if (queueCategory == "wait") {
            val removedCount = redisTemplate.opsForZSet()
                .remove("$queueType$WAIT_QUEUE", userId)

            if (removedCount == null || removedCount == 0L) {
                throw ReserveException(HttpStatus.BAD_REQUEST, ErrorCode.USER_NOT_FOUND_IN_THE_QUEUE)
            }

            kafkaProducerService.sendMessage(queueType, userId)
            log.info { "$userId 님 대기열에서 취소 완료" }
        } else {
            val removedCount = redisTemplate.opsForZSet()
                .remove("$queueType$ALLOW_QUEUE", userId)

            if (removedCount == null || removedCount == 0L) {
                throw ReserveException(HttpStatus.BAD_REQUEST, ErrorCode.USER_NOT_FOUND_IN_THE_QUEUE)
            }

            val tokenTtlKey = "token:$userId:TTL"
            try {
                redisTemplate.delete(tokenTtlKey)
                log.info { "$userId 님의 TTL 키 삭제 완료" }
            } catch (e: Exception) {
                error { "$userId 님의 TTL 키 삭제 중 오류 발생: ${e.message}" }
            }

            log.info { "$userId 님 참가열에서 취소 완료" }
        }

        changeUserStatus(queueType, userId, "canceled")
    }

    companion object {
        fun generateAccessToken(userId: String, queueType: String): String {
            return try {
                val digest = MessageDigest.getInstance("SHA-256")
                val raw = queueType + ACCESS_TOKEN + userId
                val hash = digest.digest(raw.toByteArray(StandardCharsets.UTF_8))

                hash.joinToString("") { "%02x".format(it) }
            } catch (e: NoSuchAlgorithmException) {
                throw RuntimeException("Token 생성 실패", e)
            }
        }
    }

    fun sendCookie(userId: String, queueType: String, response: HttpServletResponse): ResponseEntity<String> {
        val encodedName = URLEncoder.encode(userId, StandardCharsets.UTF_8)
        val token = generateAccessToken(userId, queueType)

        val cookie = Cookie("${queueType}_user-access-cookie_$encodedName", token)
        cookie.path = "/"
        cookie.maxAge = 300
        response.addCookie(cookie)

        return ResponseEntity.ok("쿠키 발급 완료")
    }

    fun isAccessTokenValid(userId: String, queueType: String, token: String): Boolean {
        val generatedToken = generateAccessToken(userId, queueType)

        return generatedToken == token
    }

    fun reEnterWaitQueue(userId: String, queueType: String) {
        val newTimestamp: Long = Instant.now().toEpochMilli()

        redisTemplate.opsForZSet()
            .add("$queueType$WAIT_QUEUE", userId, newTimestamp.toDouble())

        changeUserStatus(queueType, userId, "wait")
        kafkaProducerService.sendMessage(queueType, userId)
    }

    fun allowUser(queueType: String, count: Long): Long {
        val membersToAllow = redisTemplate.opsForZSet().range("$queueType$WAIT_QUEUE", 0, count - 1)
            ?: return 0L

        var allowedCount = 0L
        for (userId in membersToAllow) {
            log.info { "참가열 이동 사용자 : $userId" }
            val timestamp: Long = Instant.now().toEpochMilli()
            val tokenKey = "token:$userId:TTL"

            redisTemplate.opsForZSet()
                .add("$queueType$ALLOW_QUEUE", userId, timestamp.toDouble())

            redisTemplate.opsForValue()
                .set(tokenKey, "allowed", Duration.ofMinutes(10))

            redisTemplate.opsForZSet()
                .remove("$queueType$WAIT_QUEUE", userId)

            kafkaProducerService.sendMessage(queueType, userId)
            changeUserStatus(queueType, userId, "allow")
            allowedCount++
        }

        log.info { "참가열로 이동된 사용자 수: $allowedCount" }
        return allowedCount
    }

    @Scheduled(fixedDelay = 3000, initialDelay = 40000)
    fun moveUserToAllowQ() {
        val maxAllowedUsers = 3L
        val queueTypes = listOf("reserve")

        queueTypes.forEach { queueType ->
            val movedCount = allowUser(queueType, maxAllowedUsers)
            if (movedCount > 0) {
                log.info { "$queueType 에서 $movedCount 명의 사용자가 참가열로 이동되었습니다." }
            } else {
                log.info { "참가열로 이동된 사용자가 없습니다" }
            }
        }
    }

    @Transactional
    fun changeUserStatus(queueType: String, userId: String, status: String) {
        val user = userRepository.findByUserId(userId)
            ?.apply { updateStatus(status) }
            ?: User(userId = userId, queueType = queueType, status = status)

        userRepository.save(user)
    }
}