package com.example.orderintegration.service;

import com.example.orderintegration.model.OrderEvent;
import org.springframework.stereotype.Service;

@Service
public class OrderValidationService {
    public String validate(OrderEvent o) {
        if (o == null) return "Order payload is null";
        if (blank(o.orderId())) return "orderId is required";
        if (blank(o.customerId())) return "customerId is required";
        if (o.amount() == null || o.amount().signum() <= 0) return "amount must be greater than zero";
        if (blank(o.currency())) return "currency is required";
        if (blank(o.productCode())) return "productCode is required";
        if (o.quantity() == null || o.quantity() <= 0) return "quantity must be greater than zero";
        if (o.quantity() > 3) return "quantity exceeds the allowed maximum of 3";
        return null;
    }
    private boolean blank(String s) { return s == null || s.isBlank(); }
}
