Jmeter, Spring Actuator, Prometheus, Grafana를 통해 개발 단계별 대기열 시스템의 성능을 테스트 하고자 하는 리포지토리입니다.<br><br>

1. DB 기반 동기적인 대기열 시스템 + 대기열 멱등성 로직 ( MySQL DB )

   [ DB와 이벤트 간의 정합성이 맞지 않는 문제 ]<br><br>
   ⇒ Kafka Connect인 MySQL Debezium을 통해 해결<br><br>

   [ Number of thread : 3000 , Ramp up period : 1 ]<br>
   응답 속도 : 약 1 ~ 1.5초 , 최대 응답 속도 : 1.5 ~ 2초 , 초당 처리량 : 약 1500 TPS<br><br>


3. DB를 제거하고 WefFlux 기반 비동기 아키텍처로 변경

   기존의 사용자 상태 저장 DB를 제거하고, 직접 Kafka로 이벤트를 전달하는 방식으로 변경<br><br>

   [ Number of thread : 3000 , Ramp up period : 1 ]<br>
   응답 속도 : 약 0.5초 이하 , 최대 응답 속도 : 1초 이하 , 초당 처리량 : 약 2000 ~ 2500 TPS<br><br>

   ⇒ 이전 대비 응답 속도는 약 50%, 초당 처리량은 약 30% 개선<br><br>


5. 멱등성 로직, 멱등 키를 저장하는 DB를 R2DBC로 변경

   비동기 DB 도입에 따른 성능 변화 체크<br><br>


6. 기존 Kotlin + WebFlux 기반 코드와 Coroutine 적용 코드 간의 성능 비교<br><br>


7. 단일 서버에서 분산 환경으로 아키텍처를 변경

   단일 서버와 분산 환경에서의 성능 비교

   Kafka 토픽 파티션의 개수에 따른 처리량 비교<br>
   ( consumer group 내의 서버의 개수 이상이어야 함 )
