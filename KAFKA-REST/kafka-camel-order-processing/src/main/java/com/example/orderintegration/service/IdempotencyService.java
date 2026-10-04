package com.example.orderintegration.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class IdempotencyService {
    private final JdbcTemplate jdbc;

    public IdempotencyService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public boolean tryClaim(String orderId) {
        int inserted = jdbc.update(
            "INSERT INTO processed_orders(order_id,status) VALUES (?, 'PROCESSING') " +
            "ON CONFLICT (order_id) DO NOTHING", orderId);
        return inserted == 1;
    }

    public void mark(String orderId, String status) {
        jdbc.update(
            "UPDATE processed_orders SET status=?, updated_at=CURRENT_TIMESTAMP WHERE order_id=?",
            status, orderId);
    }
}
