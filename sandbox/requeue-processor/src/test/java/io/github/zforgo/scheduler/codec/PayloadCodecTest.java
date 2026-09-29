package io.github.zforgo.scheduler.codec;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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
