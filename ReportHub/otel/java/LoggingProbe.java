import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

public final class LoggingProbe {
    public static void main(String[] args) {
        String marker = args[0];
        int size = args.length > 1 ? Integer.parseInt(args[1]) : 0;
        Logger logger = Logger.getLogger("reporthub.logging.probe");
        MDC.put("dbz.connectorType", "probe");
        MDC.put("dbz.connectorName", "probe-connector");
        MDC.put("dbz.connectorContext", "streaming");
        IllegalStateException exception = new IllegalStateException(
            "outer failure", new IllegalArgumentException("inner cause")
        );
        exception.addSuppressed(new UnsupportedOperationException("suppressed failure"));
        logger.error(
            marker + " ERROR start\nquoted \"value\" and \\path\tcontrol \u2603"
                + " password=probe-secret WARNING DEBUG\n" + "x".repeat(size) + marker + " END",
            exception
        );
        logger.warn(marker + " WARN message without an exception");
    }
}
