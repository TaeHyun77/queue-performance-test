package com.example.QueueTest.kafka

import com.example.QueueTest.redis.subscribe.RedisPublisher

import com.example.QueueTest.util.CHANNEL_NAME
import com.example.QueueTest.util.Loggable

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Service

@Service
class KafkaConsumerService (
    private val objectMapper: ObjectMapper,
    private val redisPublisher: RedisPublisher,

    @Value("\${SERVER_NAME}")
    private val serverName: String? = null

): Loggable {

    @KafkaListener(topics = ["test_queueing_system"])
    fun consume(message: String, record: ConsumerRecord<String, String>) {
        try {
            val messageDto: KafkaMessageDto = objectMapper.readValue(message, KafkaMessageDto::class.java)
            val queueType: String = messageDto.queueType

            log.info("Kafka consume - queueType: $queueType, topic: ${record.topic()}, partition : ${record.partition()}, consume-server-name: $serverName")

            redisPublisher.publish(CHANNEL_NAME, queueType)

        } catch (e: Exception) {
            log.error(e) { "Kafka 메시지 consume 실패" };
        }
    }
}