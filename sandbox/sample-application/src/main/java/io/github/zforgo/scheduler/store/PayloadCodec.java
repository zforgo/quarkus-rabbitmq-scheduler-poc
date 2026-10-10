package io.github.zforgo.scheduler.store;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.serializer.Serializer;
import org.eclipse.serializer.SerializerFoundation;
import org.eclipse.serializer.SerializerTypeInfoStrategyCreator;
import org.eclipse.serializer.TypedSerializer;

@ApplicationScoped
public class PayloadCodec {

    private final BlockingQueue<Serializer<byte[]>> pool;
    private final SerializerFoundation<?> foundation;

    PayloadCodec(@ConfigProperty(name = "scheduler.store.pool-size", defaultValue = "8") int size) {
        foundation = SerializerFoundation.New()
                .setSerializerTypeInfoStrategyCreator(new SerializerTypeInfoStrategyCreator.IncrementalDiff(true));
        pool = IntStream.range(0, size)
                .mapToObj(_ -> TypedSerializer.Bytes(foundation))
                .collect(Collectors.toCollection(() -> new ArrayBlockingQueue<>(size)));
    }

    @FunctionalInterface
    private interface SerdeOp<P, R> {

        R apply(Serializer<byte[]> serde, P input) throws Exception;
    }

    public byte[] encode(Object payload) {
        return run(payload, Serializer::serialize, "encode");
    }

    public Object decode(byte[] bytes) {
        return run(bytes, Serializer::deserialize, "decode");
    }

    private <P, R> R run(P input, SerdeOp<P, R> op, String method) {
        Serializer<byte[]> serde;
        try {
            serde = pool.take();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PayloadCodecException(e);
        }
        try {
            return op.apply(serde, input);
        } catch (Throwable t) {
            try {
                serde.close();
            } catch (Exception closeEx) {
                t.addSuppressed(closeEx);
            }
            serde = TypedSerializer.Bytes(foundation);
            throw new PayloadCodecException("failed to " + method, t);
        } finally {
            pool.add(serde);
        }
    }

}
