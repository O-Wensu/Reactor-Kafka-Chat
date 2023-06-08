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
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.receiver.internals.ConsumerFactory;
import reactor.kafka.receiver.internals.DefaultKafkaReceiver;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaWebsocketHandler implements WebSocketHandler {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private final KafkaSender<String, String> kafkaSender;
    private final DefaultDataBufferFactory bufferFactory;
    private KafkaReceiver<String, String> kafkaReceiver;
    private Flux<ReceiverRecord<String, String>> receiverKafkaFlux = null;
    private Flux<String> unfilteredTextFlux = null;

    @Override
    public Mono<Void> handle(WebSocketSession session) {
        System.out.println("KafkaEventsWebsocketHandler:: handle");

        try {
            Map<String, String> queryParams = parseQuery(session.getHandshakeInfo().getUri().getQuery());
            String chattingAddress = queryParams.get("chattingAddress");
            log.info("chattingAddress: " + chattingAddress);
            createKafkaReceiver(chattingAddress, username);

            Flux<String> receivedMessages = session.receive().map(WebSocketMessage::getPayloadAsText);

            receivedMessages
                    .flatMap(record -> {
                        SenderRecord<String, String, Integer> senderRecord =
                                SenderRecord.create(new ProducerRecord<>(chattingAddress, null, prefixUsername), 1);
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

            final Flux<String> composedTextFlux = unfilteredTextFlux;

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

    public void createKafkaReceiver(String chattingAddress, String username) {
        System.out.println("KafkaEventsWebsocketHandler:: createKafkaReceiver");

        kafkaReceiver = KafkaReceiver(chattingAddress, username);
        receiverKafkaFlux = kafkaReceiver.receive().publish().autoConnect(0);
        unfilteredTextFlux = receiverKafkaFlux.doOnNext(r -> r.receiverOffset().acknowledge()).map(ReceiverRecord::value);
    }

    public KafkaReceiver KafkaReceiver(String chattingAddress, String username) {
        System.out.println("SpringSSEConfiguration:: KafkaReceiver");

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, username + LocalDateTime.now());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        return new DefaultKafkaReceiver(ConsumerFactory.INSTANCE, ReceiverOptions.create(props)
                .subscription(Collections.singleton(chattingAddress)));
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
