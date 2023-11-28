package uk.gov.justice.digital.services.test;

import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;

public class Fixture {

    public final static String TEST_TOKEN = "test-token";

    public static final ZoneId utcZoneId = ZoneId.of("UTC");

    public static final LocalDateTime fixedDateTime = LocalDateTime.now();

    public static Clock fixedClock = Clock.fixed(fixedDateTime.toInstant(ZoneOffset.UTC), utcZoneId);

    public static <T> Matcher<Iterable<? extends T>> containsTheSameElementsInOrderAs(List<T> expectedItems) {
        return contains(expectedItems.stream().map(Matchers::equalTo).collect(Collectors.toList()));
    }

    private Fixture() {}
}
