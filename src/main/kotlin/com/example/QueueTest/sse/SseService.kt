package com.example.QueueTest.sse

import com.example.QueueTest.queue.QueueService
import com.example.QueueTest.util.Loggable
import org.springframework.stereotype.Service
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap

@Service
class SseService(
    private val queueService: QueueService
): Loggable {
    private val emitters: MutableMap<String, SseEmitter> = ConcurrentHashMap()

    fun addEmitter(sseKey: String, emitter: SseEmitter) {
        emitters.put(sseKey, emitter)

        val (queueType, userId) = sseKey.split(":")
        log.info { "${"{}님이 참가하였습니다."} $userId"}

        // 클라이언트가 SSE 연결을 정상적으로 종료했을 때 호출
        emitter.onCompletion { emitters.remove(sseKey) }

        // 서버에 설정된 타임아웃 시간( 기본 30초 ~ 60초 )이 지나도 클라이언트로 응답을 못 보낼 경우 호출
        emitter.onTimeout { emitter.complete() }

        val rank = queueService.searchUserRanking(userId, queueType, "wait")
        if (rank != null && rank > 0) {
            this.sendTo(
                sseKey, "update", mapOf(
                    "event" to "update",
                    "rank" to rank
                )
            )
            log.info { "${"초기 rank 전송 완료: {}"} $rank"}
        } else {
            log.info { "초기 rank 전송 실패" }
        }
    }

    fun sendTo(sseKey: String, eventName: String, data: Any) {
        val emitter = emitters[sseKey]

        if (emitter != null) {
            try {
                emitter.send(
                    SseEmitter.event()
                        .name(eventName)
                        .data(data)
                )
            } catch (e: IOException) {
                emitters.remove(sseKey)
            }
        }
    }

    fun broadcastRankOrConfirm(queueType: String) {
        log.info("broadcast")

        val sseKeys = emitters.keys

        for (sseKey in sseKeys) {
            log.info("sseKey : {}", sseKey)

            val userId = sseKey.split(":")[1]

            val isExistsInWait = queueService.isExistUserInWaitOrAllow(userId, queueType, "wait")
            log.info("isExistsInWait : {}", isExistsInWait)

            // 대기열에 있다면 rank 값 반환
            if (isExistsInWait) {
                log.info("rank update")
                val rank = queueService.searchUserRanking(userId, queueType, "wait")
                log.info("rank : {}", rank)

                if (rank != null && rank > 0) {
                    this.sendTo(
                        sseKey, "update", mapOf(
                            "event" to "update",
                            "rank" to rank
                        )
                    )
                    log.info("update 전송완료")
                }

                // 참가열에 있다면 'confirm' 메세지 보냄
            } else {
                log.info("send confirm")

                this.sendTo(
                    sseKey, "confirm", mapOf(
                        "event" to "confirm",
                        "user_Id" to userId
                    )
                )
            }
        }
    }
}