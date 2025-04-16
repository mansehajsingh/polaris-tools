package org.apache.polaris.tools.sync.polaris;

import org.apache.polaris.tools.sync.polaris.catalog.NoOpETagManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class ETagManagerFactoryTest {

    @Test
    public void constructCsvManagerSuccessfully() {
        // Pass correct properties for default file etag manager
        ETagManagerFactory.createETagManager(
                ETagManagerFactory.Type.FILE, Map.of(CsvETagManager.CSV_FILE_PROPERTY, "test.csv"));
    }

    @Test
    public void failToConstructCsvManager() {
        // omit properties for default file etag manager
        Assertions.assertThrows(Exception.class, () ->
                ETagManagerFactory.createETagManager(ETagManagerFactory.Type.FILE, Map.of()));
    }

    @Test
    public void constructCustomETagManagerSuccessfully() {
        // correctly construct custom manager by passing classname
        ETagManagerFactory.createETagManager(
                ETagManagerFactory.Type.CUSTOM,
                Map.of(ETagManagerFactory.CUSTOM_CLASS_NAME_PROPERTY, NoOpETagManager.class.getName()) // use a recognized one for now
        );
    }

    @Test
    public void failToConstructCustomETagManager() {
        // fail to construct custom etag manager by omitting custom classname from config
        Assertions.assertThrows(Exception.class, () ->
            ETagManagerFactory.createETagManager(ETagManagerFactory.Type.CUSTOM, Map.of()));
    }

}
