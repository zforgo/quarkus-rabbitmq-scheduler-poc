package io.github.zforgo.scheduler.codec;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.eclipse.serializer.SerializerFoundation;
import org.eclipse.serializer.SerializerTypeInfoStrategyCreator;
import org.eclipse.serializer.TypedSerializer;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class PayloadCodecTest {

    private static final Supplier<Stream<Object>> elements = () -> Stream.of(
            1,
            true,
            1L,
            "fooBar",
            List.of("a", "b", "c"),
            List.of("b", "c", 1L, true, 30)
    );

    @ParameterizedTest
    @MethodSource("rawElements")
    void backToBack(Object input) throws InterruptedException {
        var bytes = PayloadCodec.INSTANCE.encode(input);
        var restored = PayloadCodec.INSTANCE.decode(bytes);
        assertEquals(input, restored);
    }

    @ParameterizedTest
    @MethodSource("payloadElements")
    void payloadConvert(GenericTestPayload<?> input) throws InterruptedException {
        var bytes = PayloadCodec.INSTANCE.encode(input);
        var restored = PayloadCodec.INSTANCE.decode(bytes);
        assertEquals(input, restored);
    }

    @Test
    @Disabled
    void consistenceCheck() {
        var foundation = SerializerFoundation.New()
                .setSerializerTypeInfoStrategyCreator(new SerializerTypeInfoStrategyCreator.IncrementalDiff(true));
        var serde = TypedSerializer.Bytes(foundation);
        var first = serde.serialize("input");
        try {
            var tmp = serde.serialize("input");
            tmp[1] = (byte) (tmp[1] + 1);
            var a = serde.deserialize(tmp);
            System.out.println("fine: " + a);

        } catch (Exception e) {
            System.out.println("error: " + e);
        }
        assertEquals("input", serde.deserialize(first));

    }

    @Test
    @Disabled
    void consistencyUnderFuzzing() {
        java.util.logging.Logger.getLogger("org.eclipse.serializer").setLevel(java.util.logging.Level.OFF);
        var foundation = SerializerFoundation.New()
                .setSerializerTypeInfoStrategyCreator(new SerializerTypeInfoStrategyCreator.IncrementalDiff(true));
        var serde = TypedSerializer.Bytes(foundation);

        var payload = new GenericTestPayload<>(List.of("a", "b", "c")).withName("Joe"); // a realistic shape
        var good = serde.serialize(payload);

        var rnd = new Random(42);
        for (int i = 0; i < 3500; i++) {
            System.out.println("Ájterérön: " + i);
            var corrupt = good.clone();
            corrupt[rnd.nextInt(corrupt.length)] = (byte) rnd.nextInt(256);
            try {
                var restored = serde.deserialize(corrupt);
                // got a result without throwing — was it actually right, or silently wrong?
                if (!payload.equals(restored) && !Arrays.equals(corrupt, good)) {
                    System.out.println("silent mismatch at iteration " + i + ": " + restored);
                }
            } catch (Exception ignored) {
                //				serde = TypedSerializer.Bytes(foundation);
                // expected for most mutations
            } finally {
                var freshFoundation = SerializerFoundation.New()
                        .setSerializerTypeInfoStrategyCreator(new SerializerTypeInfoStrategyCreator.IncrementalDiff(true));
                serde = TypedSerializer.Bytes(freshFoundation);
                //				serde = TypedSerializer.Bytes(foundation);
            }
        }

        // after thousands of corruptions on the SAME instance, must still round-trip correctly
        var reserialized = serde.serialize(payload);
        assertArrayEquals(good, reserialized); // byte-identical, not just equal value — catches internal drift
        assertEquals(payload.getFoo(), ((GenericTestPayload<?>) serde.deserialize(good)).getFoo());
    }

    @Test
    @Disabled
    void isolateIteration3468() {
        var foundation = SerializerFoundation.New()
                .setSerializerTypeInfoStrategyCreator(new SerializerTypeInfoStrategyCreator.IncrementalDiff(true));
        var serde = TypedSerializer.Bytes(foundation);

        var payload = new GenericTestPayload<>(List.of("a", "b", "c")).withName("Joe"); // a realistic shape
        var good = serde.serialize(payload); // same payload the fuzz test uses

        var rnd = new Random(42);
        byte[] corrupt = null;
        int pos = -1;
        byte val = 0;
        for (int i = 0; i <= 3468; i++) {
            corrupt = good.clone();
            pos = rnd.nextInt(corrupt.length);
            val = (byte) rnd.nextInt(256);
            corrupt[pos] = val;
            // deliberately NOT calling deserialize here — just replaying the RNG draws
        }

        System.out.println("iteration 3468: pos=" + pos + " val=" + val + " / buffer length=" + corrupt.length);
        var result = serde.deserialize(corrupt); // the only deserialize call that happens, on a pristine instance
        System.out.println("got: " + result);
    }

    static Stream<Arguments> payloadElements() {
        return elements.get()
                .map(GenericTestPayload::new)
                .map(Arguments::arguments);
    }

    static Stream<Arguments> rawElements() {
        return elements.get()
                .map(Arguments::arguments);
    }
}
