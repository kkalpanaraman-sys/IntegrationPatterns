package com.example.orderintegration.security;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.oauth2.client.AuthorizedClientServiceOAuth2AuthorizedClientManager;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClientManager;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClientProvider;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClientProviderBuilder;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClientService;
import org.springframework.security.oauth2.client.InMemoryOAuth2AuthorizedClientService;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.client.registration.ClientRegistrationRepository;
import org.springframework.security.oauth2.client.registration.InMemoryClientRegistrationRepository;
import org.springframework.security.oauth2.core.AuthorizationGrantType;
import org.springframework.security.oauth2.core.ClientAuthenticationMethod;
import org.springframework.util.StringUtils;

@Configuration
@ConditionalOnProperty(name = "downstream.auth-enabled", havingValue = "true")
public class OAuth2ClientConfig {

    @Bean
    ClientRegistrationRepository downstreamClientRegistrationRepository(
            @Value("${oauth2.token-uri}") String tokenUri,
            @Value("${oauth2.client-id}") String clientId,
            @Value("${oauth2.client-secret}") String clientSecret,
            @Value("${oauth2.scope:}") String scope,
            @Value("${oauth2.client-auth-method:client_secret_basic}") String authMethod) {

        ClientRegistration.Builder builder = ClientRegistration
                .withRegistrationId("downstream")
                .tokenUri(tokenUri)
                .clientId(clientId)
                .clientSecret(clientSecret)
                .clientAuthenticationMethod(authenticationMethod(authMethod))
                .authorizationGrantType(AuthorizationGrantType.CLIENT_CREDENTIALS);

        if (StringUtils.hasText(scope)) {
            builder.scope(scope.trim().split("\\s+"));
        }

        return new InMemoryClientRegistrationRepository(builder.build());
    }

    private ClientAuthenticationMethod authenticationMethod(String value) {
        return switch (value.trim().toLowerCase()) {
            case "client_secret_post" -> ClientAuthenticationMethod.CLIENT_SECRET_POST;
            case "client_secret_basic" -> ClientAuthenticationMethod.CLIENT_SECRET_BASIC;
            default -> throw new IllegalArgumentException(
                    "OAUTH2_CLIENT_AUTH_METHOD must be client_secret_basic or client_secret_post");
        };
    }

    @Bean
    OAuth2AuthorizedClientService downstreamAuthorizedClientService(
            ClientRegistrationRepository registrations) {
        return new InMemoryOAuth2AuthorizedClientService(registrations);
    }

    @Bean
    OAuth2AuthorizedClientManager downstreamAuthorizedClientManager(
            ClientRegistrationRepository registrations,
            OAuth2AuthorizedClientService authorizedClientService) {

        OAuth2AuthorizedClientProvider provider = OAuth2AuthorizedClientProviderBuilder
                .builder()
                .clientCredentials()
                .build();

        AuthorizedClientServiceOAuth2AuthorizedClientManager manager =
                new AuthorizedClientServiceOAuth2AuthorizedClientManager(
                        registrations, authorizedClientService);
        manager.setAuthorizedClientProvider(provider);
        return manager;
    }
}
