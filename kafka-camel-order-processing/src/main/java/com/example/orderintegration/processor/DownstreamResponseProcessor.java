package com.example.orderintegration.processor;

import com.example.orderintegration.model.ErrorEvent;
import com.example.orderintegration.model.OrderEvent;
import com.example.orderintegration.model.ProcessingResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class DownstreamResponseProcessor implements Processor {
    public static final String ORIGINAL_ORDER = "originalOrder";
    private final ObjectMapper mapper;

    public DownstreamResponseProcessor(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public void process(Exchange ex) throws Exception {
        OrderEvent order = ex.getProperty(ORIGINAL_ORDER, OrderEvent.class);
        Integer status = ex.getMessage().getHeader(Exchange.HTTP_RESPONSE_CODE, Integer.class);
        String body = ex.getMessage().getBody(String.class);

        if (status == null || status >= 500) {
            throw new DownstreamTransientException(
                "Transient downstream failure: HTTP " + status + " " + body);
        }

        if (status >= 200 && status < 300) {
            ex.getMessage().setBody(new ProcessingResult(
                order.orderId(), "SUCCESS", status, body, Instant.now()));
            ex.getMessage().setHeader("resultTopic", "orders.success");
            return;
        }

        if (status >= 400 && status < 500) {
            ex.getMessage().setBody(new ProcessingResult(
                order.orderId(), "REJECTED", status, body, Instant.now()));
            ex.getMessage().setHeader("resultTopic", "orders.rejected");
            return;
        }

        throw new DownstreamTransientException("Unhandled HTTP status " + status);
    }

    public String json(Object value) throws Exception {
        return mapper.writeValueAsString(value);
    }

    public String failedJson(Exchange ex, String message) throws Exception {
        OrderEvent order = ex.getProperty(ORIGINAL_ORDER, OrderEvent.class);
        Integer status = ex.getMessage().getHeader(Exchange.HTTP_RESPONSE_CODE, Integer.class);
        Integer attempt = ex.getProperty(Exchange.REDELIVERY_COUNTER, Integer.class);
        ErrorEvent event = new ErrorEvent(
            order == null ? "unknown" : order.orderId(),
            "FAILED", status, "DOWNSTREAM_TRANSIENT_FAILURE", message,
            attempt == null ? 0 : attempt, Instant.now());
        return json(event);
    }
}
