package com.example.QueueTest.idempotency

import com.example.QueueTest.util.Loggable
import com.example.integrated.reserveException.ReserveException
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import java.time.LocalDateTime

@Component
class IdempotencyService(
    private val idempotencyRepository: IdempotencyRepository,
) : Loggable {

    fun execute(
        key: String,
        url: String,
        method: String,
        process: () -> Mono<String>
    ): Mono<ResponseEntity<String>> {

        val now = LocalDateTime.now()

        return idempotencyRepository.findByIdempotencyKey(key)
            .flatMap { idempotency ->
                if (idempotency.expires_at.isAfter(now) && idempotency.responseBody == "REGISTERED") {
                    log.info { "동일한 Idempotent 요청 감지됨 - 저장된 이전 응답 반환" }
                    log.info { "⇒ 이미 대기열에 등록된 사용자입니다." }

                    Mono.just(
                        ResponseEntity
                            .status(idempotency.statusCode)
                            .body("이미 대기열에 등록된 사용자입니다.")
                    )
                } else {
                    handleProcess(key, url, method, now, process)
                }
            }
            .switchIfEmpty(
                handleProcess(key, url, method, now, process)
            )
    }

    private fun handleProcess(
        key: String,
        url: String,
        method: String,
        now: LocalDateTime,
        process: () -> Mono<String>
    ): Mono<ResponseEntity<String>> {

        return process()
            .flatMap { successMessage ->

                val newIdempotency = Idempotency(
                    idempotencyKey = key,
                    url = url,
                    httpMethod = method,
                    responseBody = successMessage,
                    statusCode = 200,
                    expires_at = now.plusMinutes(10)
                )

                idempotencyRepository.save(newIdempotency)
                    .thenReturn(ResponseEntity.status(200).body(successMessage))
            }
            .onErrorResume(ReserveException::class.java) { e ->
                val failedIdempotency = Idempotency(
                    idempotencyKey = key,
                    url = url,
                    httpMethod = method,
                    responseBody = e.errorCode.name,
                    statusCode = e.status.value(),
                    expires_at = now.plusMinutes(10)
                )

                idempotencyRepository.save(failedIdempotency)
                    .thenReturn(ResponseEntity.status(e.status).body(e.errorCode.name))
            }
    }
}