Jmeter, Spring Actuator, Prometheus, Grafana를 통해 단계별 대기열 시스템의 성능을 테스트 하고자 하는 리포지토리입니다.<br><br>

1. DB 기반 동기적인 대기열 시스템 + 대기열 멱등성 로직 ( MySQL DB )

   [ DB와 이벤트 간의 정합성이 맞지 않는 문제 ]<br><br>
   ⇒ Kafka Connect인 MySQL Debezium을 통해 해결<br><br>


2. 동기적인 대기열 시스템의 성능이 부족하여 DB를 제거하고 WefFlux 기반 비동기 아키텍처로 변경

   기존의 사용자 상태 저장 DB를 제거하고, 직접 Kafka로 이벤트를 전달하는 방식으로 변경<br><br>


3. 멱등성 로직, 멱등 키를 저장하는 DB를 R2DBC로 변경

   비동기 DB 도입에 따른 성능 변화 체크<br><br>


4. 기존 Kotlin + WebFlux 기반 코드와 Coroutine 적용 코드 간의 성능 비교<br><br>


5. 단일 서버에서 분산 환경으로 아키텍처를 변경

   단일 서버와 분산 환경에서의 성능 비교   
