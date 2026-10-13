package io.github.zforgo.scheduler.internal;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

final class UuidV7 {

    private UuidV7() {
        //avoid direct instantiation
    }

    static UUID of(long epochMillis) {
        var random = ThreadLocalRandom.current();
        //@formatter:off
        long msb = (epochMillis << 16) 								// 48-bit timestamp
                | 0x7000L 											// version 7
                | (random.nextLong() & 0x0FFFL); 					// 12 random bits (rand_a)
        long lsb = (random.nextLong() & 0x3FFF_FFFF_FFFF_FFFFL)
                | 0x8000_0000_0000_0000L; 							// variant 10 + 62 random bits (rand_b)
		//@formatter:on
        return new UUID(msb, lsb);
    }

    static UUID of(Instant instant) {
        return of(instant.toEpochMilli());
    }

    static UUID nowUTC() {
        return of(Instant.now());
    }
}
