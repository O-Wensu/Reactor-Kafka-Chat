package com.example.kafkachat.controller;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaWebsocketHandler implements WebSocketHandler {

    private final KafkaReceiver<String, String> kafkaReceiver;
    private final KafkaSender<String, String> kafkaSender;
    private final DefaultDataBufferFactory bufferFactory;
    private Flux<ReceiverRecord<String, String>> receiverKafkaFlux = null;
    private Flux<String> unfilteredTextFlux = null;

    @PostConstruct
    public void createKafkaReceiver() {
        System.out.println("KafkaEventsWebsocketHandler:: createKafkaReceiver");

        receiverKafkaFlux = kafkaReceiver.receive().publish().autoConnect(0);
        unfilteredTextFlux = receiverKafkaFlux.doOnNext(r -> r.receiverOffset().acknowledge()).map(ReceiverRecord::value);
    }

    @Override
    public Mono<Void> handle(WebSocketSession session) {
        System.out.println("KafkaEventsWebsocketHandler:: handle");

        try {
            Map<String, String> queryParams = parseQuery(session.getHandshakeInfo().getUri().getQuery());
            String chattingAddress = queryParams.get("chattingAddress");
            log.info("chattingAddress: " + chattingAddress);

            Flux<String> receivedMessages = session.receive().map(WebSocketMessage::getPayloadAsText);

            receivedMessages
                    .flatMap(record -> {
                        SenderRecord<String, String, Integer> senderRecord =
                                SenderRecord.create(new ProducerRecord<>("test", null, record), 1);
                                return kafkaSender.send(Mono.just(senderRecord))
                                .doOnNext(result -> {
                                    if (result instanceof SenderResult) {
                                        SenderResult<Integer> senderResult = (SenderResult<Integer>) result;
                                        log.info("Message sent successfully: " + senderResult.correlationMetadata());
                                    }
                                })
                                .then(Mono.just(record));
                    })
                    .doOnSubscribe(subscription -> {
                        receivedMessages.subscribe(
                                null,
                                error -> log.error("Error in receivedMessages subscription: " + error.getMessage())
                        );
                    })
                    .doOnSubscribe(subscription -> {
                        kafkaSender.send(Flux.empty()).subscribe();
                    }).subscribe();

            Flux<String> filteredTextFlux = unfilteredTextFlux;
            if (chattingAddress != null)
                filteredTextFlux = unfilteredTextFlux.filter(record -> record.contains(chattingAddress));

            final Flux<String> composedTextFlux = filteredTextFlux;

            return session.send(
                    composedTextFlux
                            .map(i -> new WebSocketMessage(WebSocketMessage.Type.TEXT,
                                    bufferFactory.wrap(ByteBuffer.wrap(i.getBytes(StandardCharsets.UTF_8)))))
                            .doOnSubscribe(subscription -> composedTextFlux.subscribe()));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return Mono.empty();
    }


    private Map<String, String> parseQuery(String query) throws UnsupportedEncodingException {
        System.out.println("KafkaEventsWebsocketHandler:: parseQuery");

        Map<String, String> queryParams = new HashMap<String, String>();

        if (query == null || query.isEmpty()) return queryParams;

        String[] params = query.split("&");

        for (String currParam : params) {
            int eqOffset = currParam.indexOf("=");
            if (eqOffset > 0 && eqOffset + 1 < currParam.length()) {
                String key = URLDecoder.decode(currParam.substring(0, eqOffset), "UTF-8");
                String value = URLDecoder.decode(currParam.substring(eqOffset + 1), "UTF-8");
                queryParams.put(key, value);
            }
        }
        return queryParams;
    }
}
