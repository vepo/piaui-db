package dev.vepo.piauidb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.nio.file.Files;

import org.junit.jupiter.api.Test;

public class PiauiDBTest {

    @Test
    public void createFromEmptyFolderTest() throws IOException {
        try (var db = PiauiDB.open(Files.createTempDirectory("folder").toString())) {
            assertNotNull(db);
            var dataCollection = db.collection("data");
            assertNotNull(dataCollection);
            
            assertThat(dataCollection.get("key-1".getBytes())).isEmpty();
            
            dataCollection.put("key-1".getBytes(), "value-1".getBytes());
            dataCollection.put("key-2".getBytes(), "value-2".getBytes());

            assertThat(dataCollection.get("key-1".getBytes()))
                    .isNotEmpty()
                    .map(data -> new String(data))
                    .hasValue("value-1");

            assertThat(dataCollection.get("key-2".getBytes()))
                    .isNotEmpty()
                    .map(data -> new String(data))
                    .hasValue("value-2");
            dataCollection.put("key-1".getBytes(), "value-1'".getBytes());

            assertThat(dataCollection.get("key-1".getBytes()))
                    .isNotEmpty()
                    .map(data -> new String(data))
                    .hasValue("value-1'");
        }
    }
}
