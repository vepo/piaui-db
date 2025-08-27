package dev.vepo.piauidb;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import dev.vepo.piauidb.exceptions.PiauiDBException;

/**
 * Hello world!
 */
public class PiauiDB implements AutoCloseable {

    private final Options options;

    private PiauiDB(Options options) {
        this.options = options;
    }

    public static PiauiDB open(String path) {
        try {
            var store = Files.getFileStore(Paths.get(path));
            if (store.isReadOnly()) {
                throw new PiauiDBException();
            }
            return new PiauiDB(new Options(Paths.get(path), store.getBlockSize()));
        } catch (IOException ioe) {
            throw new PiauiDBException();
        }
    }

    @Override
    public void close() {
    }

    public Collection collection(String name) {
        return new Collection(options.root().resolve(name));
    }
}
