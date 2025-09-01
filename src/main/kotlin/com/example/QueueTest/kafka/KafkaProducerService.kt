package com.example.QueueTest.kafka

import com.example.QueueTest.util.Loggable
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service
class KafkaProducerService (

    @Value("\${queue.event.topic.name}")
    private val topicName: String,

    private val kafkaTemplate: KafkaTemplate<String, String>,
    private val objectMapper: ObjectMapper
): Loggable {

    fun sendMessage(
        queueType: String,
        userId: String
    ): Mono<Void> {
        return Mono.fromCallable {
            val messageDto = KafkaMessageDto(queueType, userId)
            objectMapper.writeValueAsString(messageDto)
        }
            .flatMap { json ->
                val future = kafkaTemplate.send(topicName, queueType, json)
                Mono.create<Void> { sink ->
                    future.whenComplete { _, ex ->
                        if (ex == null) {
                            log.info { "Kafka produce success: $queueType - $userId" }
                            sink.success()
                        } else {
                            log.error(ex) { "Kafka produce fail: $queueType - $userId" }
                            sink.error(ex)
                        }
                    }
                }
            }
            .doOnError { e ->
                log.error(e) { "Kafka 전송 중 오류 발생" }
            }
    }
}