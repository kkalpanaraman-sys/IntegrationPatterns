package com.example.orderintegration.model;

import java.math.BigDecimal;

public record OrderEvent(
        String orderId,
        String customerId,
        BigDecimal amount,
        String currency,
        String productCode,
        Integer quantity) {}
