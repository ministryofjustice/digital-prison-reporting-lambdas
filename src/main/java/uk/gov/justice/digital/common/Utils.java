package uk.gov.justice.digital.common;

import java.util.Map;
import java.util.Optional;

public class Utils {

    public final static String DEFAULT_DPR_REGION = "eu-west-2";
    public final static String TASK_TOKEN_KEY = "token";
    public final static String REPLICATION_TASK_ARN_KEY = "replicationTaskArn";
    public final static String DELIMITER = "/";
    public final static String FILE_EXTENSION = ".parquet";

    public static Optional<String> getOptionalString(Map<String, Object> event, String key) {
        return Optional.ofNullable(event.get(key)).map(obj -> (String) obj);
    }

    private Utils() { }
}
