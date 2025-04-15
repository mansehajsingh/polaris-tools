/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
