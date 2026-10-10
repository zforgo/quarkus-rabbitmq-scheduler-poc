package io.github.zforgo.scheduler.store;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import jakarta.inject.Inject;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.quarkus.test.component.QuarkusComponentTest;
import io.quarkus.test.component.SkipInject;

import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusComponentTest(PayloadCodec.class)
public class PayloadCodecTest {

    @Inject
    PayloadCodec codec;

    private static final Supplier<Stream<Object>> elements = () -> Stream.of(
            1,
            true,
            1L,
            "fooBar",
            List.of("a", "b", "c"),
            List.of("b", "c", 1L, true, 30)
    );

    @SkipInject
    @ParameterizedTest
    @MethodSource("rawElements")
    void backToBack(Object input) {
        var bytes = codec.encode(input);
        var restored = codec.decode(bytes);
        assertEquals(input, restored);
    }

    @SkipInject
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
