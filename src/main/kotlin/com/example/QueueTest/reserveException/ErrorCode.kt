package com.example.integrated.reserveException

enum class ErrorCode (
    val errorCode: String,
    val message: String
){
    UNKNOWN("000_UNKNOWN", "알 수 없는 에러 발생"),

    ALREADY_REGISTERED_USER("ALREADY_REGISTERED_USER", "이미 등록된 유저입니다."),

    USER_NOT_FOUND_IN_THE_QUEUE("USER_NOT_FOUND_IN_THE_QUEUE", "해당 유저는 대기열에 참가되어있지 않습니다"),

    ALLOW_STATUS_JSON_EXCEPTION("ALLOW_STATUS_JSON_EXCEPTION", "허용 메세지 json 변환 실패"),

    UPDATE_STATUS_JSON_EXCEPTION("UPDATE_STATUS_JSON_EXCEPTION", "업데이트 메세지 json 변환 실패");
}