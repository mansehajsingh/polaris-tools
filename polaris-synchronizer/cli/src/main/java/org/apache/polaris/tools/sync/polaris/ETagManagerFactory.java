package org.apache.polaris.tools.sync.polaris;

import org.apache.polaris.tools.sync.polaris.catalog.ETagManager;
import org.apache.polaris.tools.sync.polaris.catalog.NoOpETagManager;

import java.util.HashMap;
import java.util.Map;

/**
 * Factory class to construct configurable {@link ETagManager} implementations.
 */
public class ETagManagerFactory {

    /**
     * Property that will hold class name for custom {@link ETagManager} implementation.
     */
    public static final String CUSTOM_CLASS_NAME_PROPERTY = "custom-impl";

    private ETagManagerFactory() {}

    /**
     * Recognized types of {@link ETagManager} implementations
     */
    public enum Type {
        NONE,
        FILE,
        CUSTOM
    }

    /**
     * Construct a new {@link ETagManager} instance.
     * @param type the recognized type of the {@link ETagManager} to construct
     * @param properties properties to use when initializing the {@link ETagManager}
     * @return the constructed and initialized {@link ETagManager}
     */
    public static ETagManager createETagManager(Type type, Map<String, String> properties) {
        try {
            properties = properties == null ? new HashMap<>() : properties;

            ETagManager manager = switch (type) {
                case NONE -> new NoOpETagManager();
                case FILE -> new CsvETagManager();
                case CUSTOM -> {
                    String customManagerClassname = properties.get(CUSTOM_CLASS_NAME_PROPERTY);

                    if (customManagerClassname == null) {
                        throw new IllegalArgumentException("Missing required property " + CUSTOM_CLASS_NAME_PROPERTY);
                    }

                    Object custom = Class.forName(customManagerClassname).getDeclaredConstructor().newInstance();

                    if (custom instanceof ETagManager customManager) {
                        yield customManager;
                    }

                    throw new InstantiationException("Custom ETagManager '" + customManagerClassname + "' does not implement ETagManager");
                }
            };

            manager.initialize(properties);
            return manager;
        } catch (Exception e) {
            throw new RuntimeException("Failed to construct ETagManager", e);
        }
    }

}
