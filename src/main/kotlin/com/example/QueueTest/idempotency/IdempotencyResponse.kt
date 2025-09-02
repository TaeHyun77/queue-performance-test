package com.example.QueueTest.idempotency

data class IdempotencyResponse (

    val statusCode: Int,

    val responseBody: String? = null,

)