package com.example.QueueTest.sse

import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter

@RestController
class SseController (
    private val sseService: SseService
) {

    @GetMapping("/connect", produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun connect(
        @RequestParam userId: String, @RequestParam queueType: String
    ): ResponseEntity<SseEmitter> {

        val emitter: SseEmitter = SseEmitter(60 * 60 * 1000L); // 1시간

        val sseKey: String = "$queueType:$userId";
        sseService.addEmitter(sseKey, emitter)

        sseService.sendTo(queueType, "connect", "connected !")

        return ResponseEntity.ok(emitter)
    }
}