package com.example.integrated.queueing.event

import com.example.QueueTest.queue.QueueService
import com.example.QueueTest.util.Loggable
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.http.codec.ServerSentEvent
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks

@Service
class SseEventService(
    private val queueService: QueueService,
    private val objectMapper: ObjectMapper,
): Loggable {

    companion object {
        val sink: Sinks.Many<QueueEventPayload> = Sinks.many().replay().limit(1)
    }

    fun streamQueueEvents(
        userId: String,
        queueType: String
    ): Flux<ServerSentEvent<String>> {

        val queueTypeBase = queueType.split(":")[0]

        return sink.asFlux()
            .flatMap { payload ->
                log.info { "sink 이벤트 수신!" }

                queueService.searchUserRanking(userId, queueTypeBase, "allow")
                    .flatMap { allowRank ->
                        if (allowRank > 0) {
                            log.info { "참가열 존재!" }
                            val json = objectMapper.writeValueAsString(
                                mapOf("event" to "confirmed", "user_id" to userId)
                            )

                            Mono.just(ServerSentEvent.builder(json).build())
                        } else {
                            log.info { "대기열 확인 중..." }
                            queueService.searchUserRanking(userId, queueTypeBase, "wait")
                                .map { waitRank ->
                                    if (waitRank <= 0) {
                                        objectMapper.writeValueAsString(
                                            mapOf(
                                                "event" to "error",
                                                "message" to "해당 사용자는 대기열에 존재하지 않습니다."
                                            )
                                        )
                                    } else {
                                        objectMapper.writeValueAsString(
                                            mapOf("event" to "update", "rank" to waitRank)
                                        )
                                    }
                                }

                                .map { json -> ServerSentEvent.builder(json).build() }
                        }
                    }
                    .onErrorResume { ex ->
                        log.error(ex) { "streamQueueEvents 처리 중 오류 발생" }
                        val errorJson = objectMapper.writeValueAsString(
                            mapOf("event" to "error", "message" to "서버 오류 발생")
                        )

                        Mono.just(ServerSentEvent.builder(errorJson).build())
                    }
            }
    }
}