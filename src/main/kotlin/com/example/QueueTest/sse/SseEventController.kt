package com.example.QueueTest.sse

import com.example.QueueTest.util.Loggable
import kotlinx.coroutines.flow.Flow
import org.springframework.http.MediaType
import org.springframework.http.codec.ServerSentEvent
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Flux

@RestController
class SseEventController(
    private val sseEventService: SseEventService
): Loggable {

    @GetMapping("/stream", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun streamQueue(
        @RequestParam userId: String,
        @RequestParam queueType: String
    ): Flow<ServerSentEvent<String>> {

        return sseEventService.streamQueueEvents(userId, queueType)
    }
}