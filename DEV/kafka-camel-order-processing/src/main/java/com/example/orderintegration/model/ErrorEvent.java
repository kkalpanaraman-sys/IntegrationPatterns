package com.example.orderintegration.model;

import java.time.Instant;

public record ErrorEvent(
        String orderId,
        String status,
        Integer downstreamStatus,
        String errorType,
        String errorMessage,
        int attempt,
        Instant failedAt) {}
