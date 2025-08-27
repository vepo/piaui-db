package dev.vepo.piauidb;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;

import dev.vepo.piauidb.exceptions.PiauiDBException;

public class Collection implements AutoCloseable {

    private RandomAccessFile recordsFile;

    public Collection(Path path) {
        if (!path.toFile().exists()) {
            path.toFile().mkdir();
        }
        try {
            recordsFile = new RandomAccessFile(path.resolve("records.dat").toFile(), "rw");
        } catch (IOException ioe) {
            throw new PiauiDBException();
        }
    }

    private static final int RECORD_INDEX_SIZE = Integer.BYTES * 2 + 1;

    private class RecordIndex {
        private byte[] indexData;

        private RecordIndex(byte[] indexData) {
            this.indexData = indexData;
        }

        public boolean isEmpty() {
            return (indexData[0] & 0x01) == 0;
        }

        public int totalSize() {
            return keySize() + valueSize();
        }

        private int valueSize() {
            return ((indexData[5] & 0xFF) << 24) |
                    ((indexData[6] & 0xFF) << 16) |
                    ((indexData[7] & 0xFF) << 8) |
                    (indexData[8] & 0xFF);
        }

        private int keySize() {
            return ((indexData[1] & 0xFF) << 24) |
                    ((indexData[2] & 0xFF) << 16) |
                    ((indexData[3] & 0xFF) << 8) |
                    (indexData[4] & 0xFF);
        }

        public void clear() {
            indexData[0] = (byte) (indexData[0] & 0xFE);
        }
    }

    public Optional<byte[]> get(byte[] key) {
        var channel = recordsFile.getChannel();
        try {
            var fileLength = recordsFile.length();
            var currentPos = 0l;
            var indexBuffer = ByteBuffer.allocate(RECORD_INDEX_SIZE);
            while (fileLength > currentPos) {
                channel.position(currentPos);
                if (channel.read(indexBuffer) > 0) {
                    indexBuffer.flip();
                    var index = new RecordIndex(indexBuffer.array());
                    if (index.isEmpty()) {
                        currentPos += RECORD_INDEX_SIZE + index.totalSize();
                    } else {
                        var keyBuffer = ByteBuffer.allocate(index.keySize());
                        if (channel.read(keyBuffer) > 0) {
                            keyBuffer.flip();
                            byte[] currentKey = keyBuffer.array();
                            if (Arrays.equals(currentKey, key)) {
                                var valueBuffer = ByteBuffer.allocate(index.valueSize());
                                if (channel.read(valueBuffer) > 0) {
                                    valueBuffer.flip();
                                    var value = valueBuffer.array();
                                    return Optional.of(value);
                                } else {
                                    throw new PiauiDBException();
                                }
                            } else {
                                currentPos += RECORD_INDEX_SIZE + index.totalSize();
                            }
                        } else {
                            throw new PiauiDBException();
                        }
                    }
                }

            }
        } catch (IOException ioe) {
            throw new PiauiDBException();
        }
        return Optional.empty();
    }

    public void put(byte[] key, byte[] value) {
        var channel = recordsFile.getChannel();
        try {
            var fileLength = recordsFile.length();
            var currentPos = 0l;
            var indexBuffer = ByteBuffer.allocate(RECORD_INDEX_SIZE);
            while (fileLength > currentPos) {
                System.out.println("Current pos=" + currentPos + " file length=" + fileLength);
                channel.position(currentPos);
                if (channel.read(indexBuffer) > 0) {
                    indexBuffer.flip();
                    var index = new RecordIndex(indexBuffer.array());
                    if (index.isEmpty()) {
                        currentPos += RECORD_INDEX_SIZE + index.totalSize();
                    } else {
                        var keyBuffer = ByteBuffer.allocate(index.keySize());
                        if (channel.read(keyBuffer) > 0) {
                            keyBuffer.flip();
                            byte[] currentKey = keyBuffer.array();
                            if (Arrays.equals(currentKey, key)) {
                                index.clear();
                                channel.position(currentPos);
                                indexBuffer.clear();
                                indexBuffer.put(index.indexData);
                                indexBuffer.flip();
                                channel.write(indexBuffer);
                            }
                            currentPos += RECORD_INDEX_SIZE + index.totalSize();
                        } else {
                            throw new PiauiDBException();
                        }
                    }
                    indexBuffer.clear();
                }
            }
            var length = RECORD_INDEX_SIZE + key.length + value.length;
            channel.position(currentPos);
            System.out.println("Writing record at " + currentPos + " with length " + length);
            var buffer = ByteBuffer.allocate(length);
            buffer.put((byte) 0x01);
            buffer.putInt(key.length);
            buffer.putInt(value.length);
            buffer.put(key);
            buffer.put(value);
            buffer.flip();
            channel.write(buffer);
        } catch (IOException ioe) {
            throw new PiauiDBException();
        }
    }

    @Override
    public void close() {
        try {
            recordsFile.close();
        } catch (IOException e) {
            // do nothing
        }
    }
}
