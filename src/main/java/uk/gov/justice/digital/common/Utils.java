package uk.gov.justice.digital.common;

import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

public class Utils {

    public final static String DEFAULT_DPR_REGION = "eu-west-2";
    public final static String TASK_TOKEN_KEY = "token";
    public final static String TOKEN_EXPIRY_DAYS_KEY = "tokenExpiryDays";
    public final static long DEFAULT_TOKEN_EXPIRY_DAYS = 7;
    public final static String REPLICATION_TASK_ARN_KEY = "replicationTaskArn";
    public final static String IGNORE_DMS_TASK_FAILURE_KEY = "ignoreDmsTaskFailure";

    public static Optional<String> getOptionalString(Map<String, Object> event, String key) {
        return Optional.ofNullable(event.get(key)).map(obj -> (String) obj);
    }

    public static boolean getBoolean(Map<String, Object> event, String key) {
        return (boolean) event.getOrDefault(key, false);
    }

    @SuppressWarnings({"unchecked", "unused"})
    public static Map<String, String> getConfig(Map<String, Object> event, String key) {
        return (Map<String, String>) event.getOrDefault(key, Collections.emptyMap());
    }

    @SuppressWarnings({"unused"})
    public static <T, O> T getOrThrow(Map<String, O> obj, String key, Class<T> type) {
        return Optional.ofNullable(type.cast(obj.get(key)))
                .orElseThrow(() -> new NoSuchElementException("Required key [" + key + "] is missing"));
    }

    private Utils() { }
}
