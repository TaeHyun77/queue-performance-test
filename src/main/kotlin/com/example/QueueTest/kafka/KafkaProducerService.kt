package com.example.QueueTest.kafka

import com.example.QueueTest.util.Loggable
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service

@Service
class KafkaProducerService (

    @Value("\${queue.event.topic.name}")
    private val topicName: String,

    private val kafkaTemplate: KafkaTemplate<String, String>,
    private val objectMapper: ObjectMapper
): Loggable {

    fun sendMessage(queueType: String, userId: String) {
        try {

            val messageDto: KafkaMessageDto = KafkaMessageDto(queueType, userId)
            val json: String = objectMapper.writeValueAsString(messageDto)

            kafkaTemplate.send(topicName, queueType, json).whenComplete { result, ex ->
                if (ex == null) {
                    log.info { "Kafka produce success" }
                }
                else {
                    log.error(ex) { "Kafka produce fail" }
                }
            }
        } catch (e: JsonProcessingException) {
            log.info { "직렬화 실패 : $e" }
        }
    }
}