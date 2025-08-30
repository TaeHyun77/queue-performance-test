package com.example.QueueTest

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

@EnableScheduling
@SpringBootApplication
class QueueTestApplication

fun main(args: Array<String>) {
	runApplication<QueueTestApplication>(*args)
}
