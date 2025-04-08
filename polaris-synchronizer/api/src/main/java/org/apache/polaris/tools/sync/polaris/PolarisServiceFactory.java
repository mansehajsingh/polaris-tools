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

import java.util.HashMap;
import java.util.Map;

import org.apache.iceberg.rest.ResourcePaths;
import org.apache.polaris.management.ApiClient;
import org.apache.polaris.management.client.PolarisManagementDefaultApi;
import org.apache.polaris.tools.sync.polaris.auth.AuthenticationProvider;

/** Used to initialize a {@link PolarisService}. */
public class PolarisServiceFactory {

  public static PolarisService newPolarisService(
          String serviceName,
          String baseUrl,
          Map<String, String> authenticationProperties
  ) {
    ApiClient client = new ApiClient();
    client.updateBaseUri(baseUrl + "/api/management/v1");

    AuthenticationProvider authProvider = new AuthenticationProvider(serviceName, authenticationProperties);

    // tag each request with a regularly refreshed token
    client.setRequestInterceptor(
            requestBuilder -> authProvider.getAuthHeaders().forEach(requestBuilder::header));

    Map<String, String> catalogProperties = new HashMap<>();
    catalogProperties.putIfAbsent("uri", baseUrl + "/api/catalog");
    catalogProperties.putIfAbsent("oauth2-server-uri", String.format(
            "%s/%s/%s", baseUrl, "api/catalog", ResourcePaths.tokens()));

    PolarisManagementDefaultApi polarisClient = new PolarisManagementDefaultApi(client);
    return new PolarisService(polarisClient, catalogProperties);
  }
}
