package lambda.test;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

public class Fixture {

    public final static String TEST_TOKEN = "test-token";
    public final static String TEST_GLUE_JOB = "test-glue-job";

    public static final String TEST_JOB_NAME = "test-job-name";

    public static final ZoneId utcZoneId = ZoneId.of("UTC");

    public static final LocalDateTime fixedDateTime = LocalDateTime.now();

    public static Clock fixedClock = Clock.fixed(fixedDateTime.toInstant(ZoneOffset.UTC), utcZoneId);

    private Fixture() {}
}
