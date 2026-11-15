package com.javacodeex.kafka.order.service;

import com.javacodeex.kafka.order.dto.OrderRequest;
import com.javacodeex.kafka.order.dto.OrderResponse;
import com.javacodeex.kafka.order.entity.Order;
import com.javacodeex.kafka.order.producer.OrderKafkaProducer;
import com.javacodeex.kafka.order.repository.OrderRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import lombok.RequiredArgsConstructor;

import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
public class OrderService {
    private final OrderRepository orderRepository;
    private final OrderKafkaProducer orderKafkaProducer;

    @Transactional
    public OrderResponse createOrder(OrderRequest request) {
        Order order = new Order(request.getProductCode(), request.getProductName(),
                request.getQuantity(), request.getPrice(), "CREATED", LocalDateTime.now());
        Order savedOrder = orderRepository.save(order);

        orderKafkaProducer.publish(savedOrder);

        return new OrderResponse(savedOrder.getId(), savedOrder.getProductCode(),
                savedOrder.getProductName(), savedOrder.getQuantity(), savedOrder.getPrice(),
                savedOrder.getStatus());
    }
}
