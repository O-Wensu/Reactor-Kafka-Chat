package com.example.kafkachat.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.server.WebSocketService;
import org.springframework.web.reactive.socket.server.support.HandshakeWebSocketService;
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter;
import org.springframework.web.reactive.socket.server.upgrade.ReactorNettyRequestUpgradeStrategy;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
public class WebsocketHandler {
    @Bean
    public HandlerMapping handlerReceiverMapping(KafkaWebsocketHandler wsHandler) {
        System.out.println("KafkaReceiverHandler : handlerMapping");
        Map<String, WebSocketHandler> map = new HashMap<>();
        map.put("/chat", wsHandler);

        SimpleUrlHandlerMapping mapping = new SimpleUrlHandlerMapping();
        mapping.setUrlMap(map);
        mapping.setOrder(Ordered.HIGHEST_PRECEDENCE);
        return mapping;
    }

    @Bean
    public WebSocketHandlerAdapter handlerAdapter() {
        System.out.println("WebsocketHandler : handlerAdapter");
        return new WebSocketHandlerAdapter(webSocketService());
    }

    @Bean
    public WebSocketService webSocketService() {
        System.out.println("WebsocketHandler : webSocketService");
        return new HandshakeWebSocketService(new ReactorNettyRequestUpgradeStrategy());
    }

    @Bean
    public DefaultDataBufferFactory bufferFactory() {
        System.out.println("WebsocketHandler : bufferFactory");
        return new DefaultDataBufferFactory();
    }
}
