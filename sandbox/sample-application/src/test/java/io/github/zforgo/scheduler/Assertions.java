package io.github.zforgo.scheduler;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class Assertions {

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static <T> T assertNotEmpty(Optional<T> actual) {
        return assertDoesNotThrow(() -> actual.orElseThrow(), "Expected a value, but Optional was empty");
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static void assertEmpty(Optional<?> actual) {
        assertTrue(actual.isEmpty(), "Expected empty Optional");
    }
}
