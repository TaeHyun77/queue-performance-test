package com.example.integrated.reserveException

enum class ErrorCode (
    val errorCode: String,
    val message: String
){
    UNKNOWN("000_UNKNOWN", "알 수 없는 에러 발생"),

    ALREADY_REGISTERED_USER("ALREADY_REGISTERED_USER", "이미 등록된 유저입니다."),

    NOT_EXIST_IN_HEADER_IDEMPOTENCY_KEY("NOT_EXIST_IN_HEADER_IDEMPOTENCY_KEY", "헤더에 IDEMPOTENCY_KEY가 포함되어 있지 않습니다."),

    INVALID_QUEUE_CATEGORY("INVALID_QUEUE_CATEGORY", "INVALID_QUEUE_CATEGORY"),

    USER_NOT_FOUND_IN_THE_QUEUE("USER_NOT_FOUND_IN_THE_QUEUE", "USER_NOT_FOUND_IN_THE_QUEUE"),

    NOT_EXIST_TTL_INFO("NOT_EXIST_TTL_INFO", "TTL 값이 존재하지 않습니다.")
}