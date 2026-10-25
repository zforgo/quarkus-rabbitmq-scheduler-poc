package io.github.zforgo.scheduler.internal.transport;

import java.nio.ByteBuffer;
import java.util.UUID;

import io.github.zforgo.scheduler.SchedulerException;

public final class QueuedItemCodec {

    private static final int SIZE = 24; // 16 bytes UUID + 8 bytes fireTime

    public static byte[] encode(QueuedItem item) {
        return ByteBuffer.allocate(SIZE)
                .putLong(item.id().getMostSignificantBits())
                .putLong(item.id().getLeastSignificantBits())
                .putLong(item.fireTime())
                .array();
    }

    public static QueuedItem decode(byte[] body) {
        if (body == null || body.length != SIZE) {
            throw new SchedulerException("Expected " + SIZE + " bytes, got " + (body == null ? "null" : body.length));
        }
        var buf = ByteBuffer.wrap(body);
        return new QueuedItem(new UUID(buf.getLong(), buf.getLong()), buf.getLong());
    }
}
