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
package org.apache.polaris.tools.sync.polaris.auth;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthConfig;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.util.ThreadPools;

/**
 * Service to provide auth headers for requests made to Polaris. Encapsulates Iceberg auth client
 * code that enables token refresh to be provided out of the box.
 */
public class AuthenticationProvider {

  /**
   * Order of token exchange preference copied over from {@link
   * org.apache.iceberg.rest.RESTSessionCatalog}.
   */
  private static final Set<String> TOKEN_PREFERENCE_ORDER =
      Set.of(
          OAuth2Properties.ID_TOKEN_TYPE,
          OAuth2Properties.ACCESS_TOKEN_TYPE,
          OAuth2Properties.JWT_TOKEN_TYPE,
          OAuth2Properties.SAML2_TOKEN_TYPE,
          OAuth2Properties.SAML1_TOKEN_TYPE);

  // We need this in order to label the token refresh scheduler threadpool
  private final String serviceName;

  private final OAuth2Util.AuthSession authSession;

  public AuthenticationProvider(String serviceName, Map<String, String> properties) {
    this.serviceName = serviceName;
    this.authSession = this.initalizeAuthSession(properties);
  }

  /**
   * Extract out custom headers passed in as properties. Any property prefixed with "header." will
   * be extracted into the result.
   *
   * @param properties the provided properties
   * @return the custom HTTP headers provided in the properties
   */
  private Map<String, String> extractCustomHeaders(Map<String, String> properties) {
    Map<String, String> customHeaders = new HashMap<>();

    properties.forEach(
        (key, value) -> {
          if (key.startsWith("header.")) {
            customHeaders.put(key.substring("header.".length()), value);
          }
        });

    return customHeaders;
  }

  /**
   * Initialize an authentication session (that refreshes itself, if applicable).
   *
   * @param properties the authentication and HTTP properties to configure the session with
   * @return the configured session
   */
  private OAuth2Util.AuthSession initalizeAuthSession(Map<String, String> properties) {
    // Regular provided bearer token
    if (properties.containsKey(OAuth2Properties.TOKEN)) {
      return new OAuth2Util.AuthSession(
          extractCustomHeaders(properties),
          AuthConfig.builder().token(properties.get(OAuth2Properties.TOKEN)).build());
    }

    RESTClient restClient =
        HTTPClient.builder(Map.of())
            .uri(properties.get(OAuth2Properties.OAUTH2_SERVER_URI))
            .build();

    OAuth2Util.AuthSession parent =
        new OAuth2Util.AuthSession(
            extractCustomHeaders(properties),
            AuthConfig.builder()
                .credential(properties.get(OAuth2Properties.CREDENTIAL))
                .scope(properties.get(OAuth2Properties.SCOPE))
                .oauth2ServerUri(properties.get(OAuth2Properties.OAUTH2_SERVER_URI))
                .optionalOAuthParams(OAuth2Util.buildOptionalParam(properties))
                .build());

    // This is for client_credentials flow
    if (properties.containsKey(OAuth2Properties.CREDENTIAL)) {
      return OAuth2Util.AuthSession.fromCredential(
          restClient,
          ThreadPools.newScheduledPool(serviceName + "service-admin-token-refresh", 1),
          properties.get(OAuth2Properties.CREDENTIAL),
          parent);
    }

    // This is for token exchange flow
    for (String tokenType : TOKEN_PREFERENCE_ORDER) {
      if (properties.containsKey(tokenType)) {
        return OAuth2Util.AuthSession.fromTokenExchange(
            restClient,
            ThreadPools.newScheduledPool(serviceName + "service-admin-token-exchange", 1),
            properties.get(tokenType),
            tokenType,
            parent);
      }
    }

    throw new IllegalArgumentException(
        "Unable to construct authentication provider with the provided properties.");
  }

  public Map<String, String> getAuthHeaders() {
    return this.authSession.headers();
  }
}
