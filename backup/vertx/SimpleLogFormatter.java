package com.vertx;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

// Custom log formatter for simplicity
public class SimpleLogFormatter extends Formatter {
    private static final DateTimeFormatter dateTimeFormatter = 
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    @Override
    public String format(LogRecord record) {
        String timestamp = dateTimeFormatter.format(Instant.ofEpochMilli(record.getMillis()));
        return String.format("%s: %s%n", timestamp, record.getMessage());
    }
}