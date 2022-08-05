package com.kafka.sreamsImpl.kafkaConfig.chapter5;

import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

public class MyExceptionHandler implements Thread.UncaughtExceptionHandler, StreamsUncaughtExceptionHandler {
    @Override
    public void uncaughtException(Thread t, Throwable e) {
        System.out.println(t);
    }

    @Override
    public StreamThreadExceptionResponse handle(Throwable exception) {

        return StreamThreadExceptionResponse.REPLACE_THREAD;
    }
}
