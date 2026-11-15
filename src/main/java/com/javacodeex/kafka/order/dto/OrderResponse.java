package com.javacodeex.kafka.order.dto;

import java.math.BigDecimal;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderResponse {
    private Long id;
    private String productCode;
    private String productName;
    private Integer quantity;
    private BigDecimal price;
    private String status;

}
