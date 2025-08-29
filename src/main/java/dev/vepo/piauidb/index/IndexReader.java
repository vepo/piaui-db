package dev.vepo.piauidb.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Optional;

public class IndexReader {

    private static final int RECORD_INDEX_SIZE = Integer.BYTES * 2 + 1;
    private final ByteBuffer buffer;
    private final FileChannel channel;

    public IndexReader(FileChannel channel) {
        buffer = ByteBuffer.allocate(RECORD_INDEX_SIZE);
        this.channel = channel;
    }

    public Optional<Item> read() throws IOException {
        long position = channel.position();
        buffer.clear();
        if (channel.read(buffer) > 0) {
            buffer.flip();
            return Optional.of(new Item(position, buffer.array()));
        } else {
            return Optional.empty();
        }
    }

    public class Item {
        private final byte[] indexData;
        private final long position;

        private Item(long position, byte[] indexData) {
            this.position = position;
            this.indexData = indexData;
        }

        public boolean isEmpty() {
            return (indexData[0] & 0x01) == 0;
        }

        public int totalSize() {
            return RECORD_INDEX_SIZE + keySize() + valueSize();
        }

        public int valueSize() {
            return ((indexData[5] & 0xFF) << 24) |
                    ((indexData[6] & 0xFF) << 16) |
                    ((indexData[7] & 0xFF) << 8) |
                    (indexData[8] & 0xFF);
        }

        public int keySize() {
            return ((indexData[1] & 0xFF) << 24) |
                    ((indexData[2] & 0xFF) << 16) |
                    ((indexData[3] & 0xFF) << 8) |
                    (indexData[4] & 0xFF);
        }

        public void clear() throws IOException {
            indexData[0] = (byte) (indexData[0] & 0xFE);
            channel.position(position);
            buffer.clear();
            buffer.put(indexData);
            buffer.flip();
            channel.write(buffer);
        }
    }

    public static void write(FileChannel channel, byte[] key, byte[] value) throws IOException {
        var length = RECORD_INDEX_SIZE + key.length + value.length;
        var buffer = ByteBuffer.allocate(length);
        buffer.put((byte) 0x01);
        buffer.putInt(key.length);
        buffer.putInt(value.length);
        buffer.put(key);
        buffer.put(value);
        buffer.flip();
        channel.write(buffer);
    }
}