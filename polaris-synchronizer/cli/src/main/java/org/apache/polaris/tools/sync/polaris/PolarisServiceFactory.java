package org.apache.polaris.tools.sync.polaris;

import org.apache.polaris.tools.sync.polaris.service.PolarisService;
import org.apache.polaris.tools.sync.polaris.service.impl.PolarisApiService;

import java.util.Map;

public class PolarisServiceFactory {

    public enum ServiceType {
        API
    }

    public enum EndpointType {
        SOURCE,
        TARGET
    }

    public static PolarisService createPolarisService(
            ServiceType serviceType,
            EndpointType endpointType,
            Map<String, String> properties
    ) {
        PolarisService service = switch (serviceType) {
            case API -> {
                properties.putIfAbsent("iceberg-write-access", String.valueOf(endpointType == EndpointType.TARGET));
                yield new PolarisApiService();
            }
        };

        try {
            service.initialize(properties);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return service;
    }

}
