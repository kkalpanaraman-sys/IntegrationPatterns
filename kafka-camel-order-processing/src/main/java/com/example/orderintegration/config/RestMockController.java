package com.example.orderintegration.config;

import com.example.orderintegration.model.DownstreamOrderRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@RestController
@RequestMapping("/mock/downstream")
public class RestMockController {

    // Demo only: represents duplicate protection owned by the downstream system.
    // A real target would persist idempotency keys durably.
    private final Set<String> processedOrderIds = ConcurrentHashMap.newKeySet();

    @PostMapping("/orders")
    public ResponseEntity<?> submit(
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody DownstreamOrderRequest r) {

        if ("INVALID".equalsIgnoreCase(r.product()))
            return ResponseEntity.badRequest().body(Map.of("error", "Invalid product"));

        if ("FAIL-500".equalsIgnoreCase(r.product()))
            return ResponseEntity.internalServerError().body(Map.of("error", "Simulated 500"));

        if ("FAIL-503".equalsIgnoreCase(r.product()))
            return ResponseEntity.status(503).body(Map.of("error", "Simulated 503"));

        String key = idempotencyKey == null || idempotencyKey.isBlank()
                ? r.orderId()
                : idempotencyKey;

        if (!processedOrderIds.add(key)) {
            return ResponseEntity.ok(Map.of(
                    "orderId", r.orderId(),
                    "status", "ALREADY_PROCESSED"
            ));
        }

        return ResponseEntity.ok(Map.of(
                "orderId", r.orderId(),
                "status", "ACCEPTED"
        ));
    }
}
