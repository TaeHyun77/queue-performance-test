package com.example.integrated.reserveException

enum class ErrorCode (
    val errorCode: String,
    val message: String
){
    UNKNOWN("000_UNKNOWN", "알 수 없는 에러 발생"),

    ALREADY_REGISTERED_USER("ALREADY_REGISTERED_USER", "이미 등록된 유저입니다."),

    USER_NOT_FOUND_IN_THE_QUEUE("USER_NOT_FOUND_IN_THE_QUEUE", "해당 유저는 대기열에 참가되어있지 않습니다"),
}