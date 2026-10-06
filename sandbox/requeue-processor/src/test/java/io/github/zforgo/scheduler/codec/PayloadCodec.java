package io.github.zforgo.scheduler.codec;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.eclipse.serializer.Serializer;
import org.eclipse.serializer.SerializerFoundation;
import org.eclipse.serializer.SerializerTypeInfoStrategyCreator;
import org.eclipse.serializer.TypedSerializer;

public class PayloadCodec {

    public static final PayloadCodec INSTANCE = new PayloadCodec();

    private final BlockingQueue<Serializer<byte[]>> pool;
    private final SerializerFoundation<?> foundation;

    private PayloadCodec() {
        final var size = 5;
        foundation = SerializerFoundation.New()
                .setSerializerTypeInfoStrategyCreator(new SerializerTypeInfoStrategyCreator.IncrementalDiff(true));
        pool = new ArrayBlockingQueue<>(size);
        for (var i = 0; i < size; i++) {
            pool.add(TypedSerializer.Bytes(foundation));
        }

    }

    public byte[] encode(Object payload) throws InterruptedException {
        //noinspection resource in SE environment finally branch stays the pool correct, in CDI the strategy will change
        var s = pool.take();
        var success = false;
        try {
            var bytes = s.serialize(payload);
            success = true;
            return bytes;
        } finally {
            pool.add(success ? s : TypedSerializer.Bytes(foundation));
        }
    }

    public Object decode(byte[] encoded) throws InterruptedException {
        //noinspection resource in SE environment finally branch stays the pool correct, in CDI the strategy will change
        Serializer<byte[]> s = pool.take();
        var success = false;
        try {
            var result = s.deserialize(encoded);
            success = true;
            return result;
        } finally {
            pool.add(success ? s : TypedSerializer.Bytes(foundation));
        }
    }
}
