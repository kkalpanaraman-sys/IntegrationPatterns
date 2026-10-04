package com.example.orderintegration.service;

import com.example.orderintegration.model.DownstreamOrderRequest;
import com.example.orderintegration.model.OrderEvent;
import org.springframework.stereotype.Service;

@Service
public class OrderTransformationService {
    public DownstreamOrderRequest transform(OrderEvent o) {
        return new DownstreamOrderRequest(
                o.orderId(), o.customerId(), o.amount(), o.currency(),
                o.productCode(), o.quantity(), o.orderId());
    }
}
