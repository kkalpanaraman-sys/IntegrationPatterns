package com.example.orderintegration.processor;

public class DownstreamTransientException extends RuntimeException {
    public DownstreamTransientException(String message) {
        super(message);
    }
}
