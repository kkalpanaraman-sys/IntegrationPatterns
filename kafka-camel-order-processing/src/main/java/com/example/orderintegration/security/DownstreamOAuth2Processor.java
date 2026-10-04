package com.example.orderintegration.security;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.oauth2.client.OAuth2AuthorizeRequest;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClient;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClientManager;
import org.springframework.stereotype.Component;

@Component
public class DownstreamOAuth2Processor implements Processor {
    private final boolean authEnabled;
    private final ObjectProvider<OAuth2AuthorizedClientManager> managerProvider;

    public DownstreamOAuth2Processor(
            @Value("${downstream.auth-enabled:false}") boolean authEnabled,
            ObjectProvider<OAuth2AuthorizedClientManager> managerProvider) {
        this.authEnabled = authEnabled;
        this.managerProvider = managerProvider;
    }

    @Override
    public void process(Exchange exchange) {
        if (!authEnabled) {
            return;
        }

        OAuth2AuthorizedClientManager manager = managerProvider.getIfAvailable();
        if (manager == null) {
            throw new IllegalStateException(
                    "OAuth2 is enabled but the downstream OAuth2 client is not configured");
        }

        OAuth2AuthorizedClient client = manager.authorize(
                OAuth2AuthorizeRequest.withClientRegistrationId("downstream")
                        .principal("kafka-camel-order-integration")
                        .build());

        if (client == null || client.getAccessToken() == null) {
            throw new IllegalStateException("OAuth2 authorization did not return an access token");
        }

        exchange.getMessage().setHeader("Authorization",
                "Bearer " + client.getAccessToken().getTokenValue());
    }
}
