package io.github.zforgo.scheduler.internal.store;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PayloadCodecTest {

    private final PayloadCodec codec = new PayloadCodec(8);

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
    void backToBack(Object input) {
        var bytes = codec.encode(input);
        var restored = codec.decode(bytes);
        assertEquals(input, restored);
    }

    @ParameterizedTest
    @MethodSource("payloadElements")
    void payloadConvert(GenericTestPayload<?> input) {
        var bytes = codec.encode(input);
        var restored = codec.decode(bytes);
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
