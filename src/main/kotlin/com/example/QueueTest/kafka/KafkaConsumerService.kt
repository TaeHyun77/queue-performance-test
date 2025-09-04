package com.example.QueueTest.kafka

import com.example.QueueTest.sse.QueueEventPayload
import com.example.QueueTest.sse.SseEventService
import com.example.QueueTest.util.Loggable

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Service

@Service
class KafkaConsumerService (
    private val objectMapper: ObjectMapper
): Loggable {

    @KafkaListener(topics = ["test_queueing_system"])
    fun consume(message: String) {
        try {
            val messageDto: KafkaMessageDto = objectMapper.readValue(message, KafkaMessageDto::class.java)
            val queueType: String = messageDto.queueType

            SseEventService.sink.tryEmitNext(QueueEventPayload(queueType))

            log.info { "${"Kafka consume - queueType: {}"} $queueType"};
        } catch (e: Exception) {
            log.error(e) { "Kafka 메시지 consume 실패" };
        }
    }
}