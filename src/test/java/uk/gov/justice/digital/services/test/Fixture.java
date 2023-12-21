package uk.gov.justice.digital.services.test;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

public class Fixture {

    public static final ZoneId utcZoneId = ZoneId.of("UTC");

    public static final LocalDateTime fixedDateTime = LocalDateTime.now();

    public static Clock fixedClock = Clock.fixed(fixedDateTime.toInstant(ZoneOffset.UTC), utcZoneId);

    private Fixture() {}
}
