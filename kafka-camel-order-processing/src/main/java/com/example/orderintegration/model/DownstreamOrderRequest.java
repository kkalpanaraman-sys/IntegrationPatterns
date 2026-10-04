package com.example.orderintegration.model;

import java.math.BigDecimal;

public record DownstreamOrderRequest(
        String orderId,
        String customerReference,
        BigDecimal amount,
        String currency,
        String product,
        Integer quantity,
        String idempotencyKey) {}
