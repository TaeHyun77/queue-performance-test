package com.example.QueueTest.util

import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging

interface Loggable {

    // 인터페이스에서 필드를 값으로 바로 초기화 할 수 없기에, getter를 사용하여 접근할 때마다 반환하도록 함
    val log: KLogger
        get() = KotlinLogging.logger(this::class.java.simpleName)
}