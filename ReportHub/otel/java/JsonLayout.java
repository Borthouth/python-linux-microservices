package reporthub.logging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.log4j.Layout;
import org.apache.log4j.spi.LoggingEvent;

public final class JsonLayout extends Layout {
    private static final ObjectMapper JSON = new ObjectMapper();

    @Override
    public String format(LoggingEvent event) {
        String message = event.getRenderedMessage();
        if (message == null) {
            message = "";
        }
        String[] throwable = event.getThrowableStrRep();
        if (throwable != null) {
            message += "\n" + String.join("\n", throwable);
        }
        Map<String, String> record = new LinkedHashMap<>();
        record.put("schema", "reporthub.log.v1");
        record.put("timestamp", Instant.ofEpochMilli(event.getTimeStamp()).toString());
        record.put("severity", event.getLevel().toString());
        record.put("logger", event.getLoggerName());
        record.put("message", message);
        try {
            return JSON.writeValueAsString(record) + "\n";
        } catch (JsonProcessingException exc) {
            throw new IllegalStateException("Could not encode Kafka log record.", exc);
        }
    }

    @Override
    public boolean ignoresThrowable() {
        return false;
    }

    @Override
    public void activateOptions() {
    }
}
