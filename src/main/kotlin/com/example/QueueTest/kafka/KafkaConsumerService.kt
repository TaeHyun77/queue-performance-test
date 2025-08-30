package com.example.QueueTest.kafka

import com.example.QueueTest.util.Loggable
import com.example.QueueTest.sse.SseService
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Service

@Service
class KafkaConsumerService (
    private val objectMapper: ObjectMapper,
    private val sseService: SseService
): Loggable {

    @KafkaListener(topics = ["test_queueing_system"])
    fun consume(message: String) {
        try {
            val messageDto: KafkaMessageDto = objectMapper.readValue(message, KafkaMessageDto::class.java)

            val queueType: String = messageDto.queueType
            val userId: String = messageDto.userId

            sseService.broadcastRankOrConfirm(queueType)
            log.info { "${"Kafka consume - queueType: {} , userId : {}"} $queueType $userId"};
        } catch (e: Exception) {
            log.error(e) { "Kafka 메시지 consume 실패" };
        }
    }
}