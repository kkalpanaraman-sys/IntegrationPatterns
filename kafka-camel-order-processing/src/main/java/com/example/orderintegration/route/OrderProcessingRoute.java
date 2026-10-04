package com.example.orderintegration.route;

import com.example.orderintegration.model.ErrorEvent;
import com.example.orderintegration.model.OrderEvent;
import com.example.orderintegration.processor.DownstreamResponseProcessor;
import com.example.orderintegration.processor.DownstreamTransientException;
import com.example.orderintegration.service.OrderTransformationService;
import com.example.orderintegration.service.OrderValidationService;
import com.example.orderintegration.security.DownstreamOAuth2Processor;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class OrderProcessingRoute extends RouteBuilder {
    private final OrderValidationService validation;
    private final OrderTransformationService transformation;
    private final DownstreamResponseProcessor response;
    private final DownstreamOAuth2Processor oauth2;

    @Value("${downstream.max-concurrency:1000}")
    private int maxConcurrency;

    public OrderProcessingRoute(
            OrderValidationService validation,
            OrderTransformationService transformation,
            DownstreamResponseProcessor response,
            DownstreamOAuth2Processor oauth2) {
        this.validation = validation;
        this.transformation = transformation;
        this.response = response;
        this.oauth2 = oauth2;
    }

    @Override
    public void configure() {

        onException(DownstreamTransientException.class)
            .maximumRedeliveries("{{retry.max-redeliveries:3}}")
            .redeliveryDelay("{{retry.delay-ms:1000}}")
            .useExponentialBackOff()
            .backOffMultiplier(2.0)
            .retryAttemptedLogLevel(LoggingLevel.WARN)
            .handled(true)
            .process(ex -> {
                String msg = ex.getProperty(
                    org.apache.camel.Exchange.EXCEPTION_CAUGHT, Exception.class) != null
                    ? ex.getProperty(org.apache.camel.Exchange.EXCEPTION_CAUGHT, Exception.class).getMessage()
                    : "Downstream transient failure";

                ex.getMessage().setBody(response.failedJson(ex, msg));
            })
            .to("kafka:orders.failed?brokers={{kafka.bootstrap-servers}}");

        from("kafka:orders.in?brokers={{kafka.bootstrap-servers}}" +
             "&groupId={{kafka.consumer.group-id}}" +
             "&autoOffsetReset={{kafka.consumer.auto-offset-reset}}" +
             "&consumersCount={{kafka.consumer.consumers}}")
            .routeId("orders-from-kafka")
            .unmarshal().json(JsonLibrary.Jackson, OrderEvent.class)
            .process(ex -> {
                OrderEvent order = ex.getMessage().getBody(OrderEvent.class);
                ex.setProperty(DownstreamResponseProcessor.ORIGINAL_ORDER, order);

                String error = validation.validate(order);
                if (error != null) {
                    ex.setProperty("validationError", error);
                }
            })
            .choice()
                .when(simple("${exchangeProperty.validationError} != null"))
                    .process(ex -> {
                        OrderEvent order = ex.getProperty(
                            DownstreamResponseProcessor.ORIGINAL_ORDER, OrderEvent.class);
                        ErrorEvent event = new ErrorEvent(
                            order == null ? "unknown" : order.orderId(),
                            "REJECTED", 400, "VALIDATION_ERROR",
                            ex.getProperty("validationError", String.class),
                            0, Instant.now());
                        ex.getMessage().setBody(response.json(event));
                    })
                    .to("kafka:orders.rejected?brokers={{kafka.bootstrap-servers}}")
                    .stop()
                .otherwise()
                    .process(ex -> {
                        OrderEvent order = ex.getProperty(
                            DownstreamResponseProcessor.ORIGINAL_ORDER, OrderEvent.class);
                        ex.getMessage().setBody(transformation.transform(order));
                    })
                    .to("seda:downstream?blockWhenFull=true&size={{downstream.queue-size}}")
            .end();

        from("seda:downstream?concurrentConsumers={{downstream.max-concurrency}}&blockWhenFull=true&size={{downstream.queue-size}}")
            .routeId("bounded-downstream-workers")
            .process(oauth2)
            .setHeader("Content-Type", constant("application/json"))
            .setHeader("Idempotency-Key", simple("${body.orderId}"))
            .marshal().json(JsonLibrary.Jackson)
            .toD("{{downstream.endpoint-url}}?throwExceptionOnFailure=false")
            .process(response)
            .choice()
                .when(header("resultTopic").isEqualTo("orders.success"))
                    .marshal().json(JsonLibrary.Jackson)
                    .to("kafka:orders.success?brokers={{kafka.bootstrap-servers}}")
                .when(header("resultTopic").isEqualTo("orders.rejected"))
                    .marshal().json(JsonLibrary.Jackson)
                    .to("kafka:orders.rejected?brokers={{kafka.bootstrap-servers}}")
            .end();
    }
}
