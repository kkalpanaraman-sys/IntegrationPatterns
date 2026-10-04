package com.example.orderintegration.config;

import com.example.orderintegration.model.DownstreamOrderRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/mock/downstream")
public class RestMockController {
    @PostMapping("/orders")
    public ResponseEntity<?> submit(@RequestBody DownstreamOrderRequest r) {
        if ("INVALID".equalsIgnoreCase(r.product()))
            return ResponseEntity.badRequest().body(Map.of("error", "Invalid product"));
        if ("FAIL-500".equalsIgnoreCase(r.product()))
            return ResponseEntity.internalServerError().body(Map.of("error", "Simulated 500"));
        if ("FAIL-503".equalsIgnoreCase(r.product()))
            return ResponseEntity.status(503).body(Map.of("error", "Simulated 503"));
        return ResponseEntity.ok(Map.of("orderId", r.orderId(), "status", "ACCEPTED"));
    }
}
