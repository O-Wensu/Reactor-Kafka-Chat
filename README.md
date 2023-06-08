### Chat Service Using
### Reactor-Kafka, Reactive-Websocket, Spring Webflux
***
- 코드를 돌리기 전에, 먼저 kafka와 zookeeper가 설치되어 있어야 합니다.
---
- 설치 후, 아래의 명령어를 사용하여 zookeeper와 kafka를 먼저 실행시키세요. (kafka가 설치된 경로에서)
>.\bin\windows\zookeeper-server-start.bat config\zookeeper.properties
.\bin\windows\kafka-server-start.bat config\server.properties
---
- 실행 전, kafka에 "test" 토픽을 생성하고 채팅을 시작하세요. 토픽은 코드 내에서 임의로 수정할 수 있습니다.
>.\kafka-topics.bat --create --topic test --bootstrap-server localhost:9092 --partition 1
