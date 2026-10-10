package com.example.orderintegration.model;

import java.time.Instant;

public record ProcessingResult(
        String orderId,
        String status,
        Integer downstreamStatus,
        String message,
        Instant processedAt) {}
