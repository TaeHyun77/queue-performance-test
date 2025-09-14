Jmeter, Spring Actuator, Prometheus, Grafana를 통해 개발 단계별 대기열 시스템의 성능을 테스트 하고자 하는 리포지토리입니다.<br><br>

1. DB를 사용한 CDC 기반 비동기 대기열 시스템 ( MySQL DB )

   [ DB와 이벤트 간의 정합성이 맞지 않는 문제 ]<br><br>
   ⇒ Kafka Connect인 MySQL Debezium을 통해 해결<br><br>

   **[ Number of thread : 3000 , Ramp up period : 1 ]**<br><br>
   응답 속도 : 약 1 ~ 1.5초 , 최대 응답 속도 : 1.5 ~ 2초 , 초당 처리량 : 약 1500 TPS<br><br>


2. DB를 제거하고 WefFlux 기반 비동기 아키텍처로 변경

   기존의 사용자 상태 저장 DB를 제거하고, 직접 Kafka로 이벤트를 전달하는 방식으로 변경<br><br>

   **[ Number of thread : 3000 , Ramp up period : 1 ]**<br><br>
   응답 속도 : 약 0.5초 이하 , 최대 응답 속도 : 1초 이하 , 초당 처리량 : 약 2000 ~ 2500 TPS<br><br>

   ⇒ 이전 대비 응답 속도는 약 50%, 초당 처리량은 약 30% 개선<br><br>
   

3. 단일 서버에서 분산 환경으로 아키텍처를 변경

   단일 서버에서 발생하던 간헐적 에러 현상 해결 여부 확인 ( o )

   <img width="1490" height="466" alt="스크린샷 2025-09-14 오후 2 24 14" src="https://github.com/user-attachments/assets/095dd052-c354-41d3-9a90-441ed3724ef1" />

   <img width="1490" height="466" alt="스크린샷 2025-09-14 오후 2 24 19" src="https://github.com/user-attachments/assets/a251aa07-4d9f-479a-b6e5-e7f42526e99a" />

   <img width="1490" height="466" alt="스크린샷 2025-09-14 오후 2 24 22" src="https://github.com/user-attachments/assets/05a0ac32-833d-4c9e-8271-57debb21205c" />
