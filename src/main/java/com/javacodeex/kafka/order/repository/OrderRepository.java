package com.javacodeex.kafka.order.repository;

import com.javacodeex.kafka.order.entity.Order;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<Order, Long> {
}
