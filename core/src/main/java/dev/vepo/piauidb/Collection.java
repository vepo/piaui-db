package dev.vepo.piauidb;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.vepo.piauidb.exceptions.PiauiDBException;
import dev.vepo.piauidb.index.IndexReader;

public class Collection implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(Collection.class);

    private RandomAccessFile recordsFile;

    public Collection(Path path) {
        logger.info("Initializing collection... path={}", path);
        if (!path.toFile().exists()) {
            path.toFile().mkdir();
        }
        try {
            recordsFile = new RandomAccessFile(path.resolve("records.dat").toFile(), "rw");
        } catch (IOException ioe) {
            throw new PiauiDBException("Error initializing collection!", ioe);
        }
    }

    public Optional<byte[]> get(byte[] key) {
        logger.info("Requesting key={}", key);
        var channel = recordsFile.getChannel();
        try {
            var reader = new IndexReader(channel);
            var fileLength = recordsFile.length();
            var currentPos = 0l;
            while (fileLength > currentPos) {
                channel.position(currentPos);
                var maybeIndex = reader.read();
                if (maybeIndex.isPresent()) {
                    var index = maybeIndex.get();
                    if (index.isEmpty()) {
                        currentPos += index.totalSize();
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
                                    logger.warn("Value is not complete at record! index={}", index);
                                    throw new PiauiDBException("Value is not complete at record!");
                                }
                            } else {
                                currentPos += index.totalSize();
                            }
                        } else {
                            logger.warn("Key is not complete at record! index={}", index);
                            throw new PiauiDBException("Key is not complete at record!");
                        }
                    }
                }

            }
        } catch (IOException ioe) {
            throw new PiauiDBException("Error reading collection!");
        }
        return Optional.empty();
    }

    public void put(byte[] key, byte[] value) {
        logger.info("Storing key={} value={}", key, value);
        var channel = recordsFile.getChannel();
        try {
            var fileLength = recordsFile.length();
            var currentPos = 0l;
            var reader = new IndexReader(channel);
            while (fileLength > currentPos) {
                channel.position(currentPos);
                var maybeIndex = reader.read();
                if (maybeIndex.isPresent()) {
                    var index = maybeIndex.get();
                    if (index.isEmpty()) {
                        currentPos += index.totalSize();
                    } else {
                        var keyBuffer = ByteBuffer.allocate(index.keySize());
                        if (channel.read(keyBuffer) > 0) {
                            keyBuffer.flip();
                            byte[] currentKey = keyBuffer.array();
                            if (Arrays.equals(currentKey, key)) {
                                index.clear();
                            }
                            currentPos += index.totalSize();
                        } else {
                            logger.warn("Key is not complete at record! index={}", index);
                            throw new PiauiDBException("Key is not complete at record!");
                        }
                    }
                }
            }
            channel.position(currentPos);
            IndexReader.write(channel, key, value);
        } catch (IOException ioe) {
            throw new PiauiDBException("Error writing record!");
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
