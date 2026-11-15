package com.javacodeex.kafka.order.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import java.math.BigDecimal;

@Getter
@Setter
@NoArgsConstructor
public class OrderRequest {
    @NotBlank private String productCode;
    @NotBlank private String productName;
    @NotNull @Min(1) private Integer quantity;
    @NotNull @DecimalMin(value = "0.01") private BigDecimal price;

}
