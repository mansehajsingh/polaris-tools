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

import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthConfig;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.util.ThreadPools;

import java.util.Map;
import java.util.Set;

public class AuthenticationProvider {

    private static final Set<String> TOKEN_PREFERENCE_ORDER = Set.of(
            OAuthTokenType.ID_TOKEN.value(),
            OAuthTokenType.ACCESS_TOKEN.value(),
            OAuthTokenType.JWT.value(),
            OAuthTokenType.SAML2.value(),
            OAuthTokenType.SAML1.value()
    );

    private final OAuth2Util.AuthSession authSession;

    public AuthenticationProvider(Map<String, String> properties) {
        this.authSession = this.initalizeAuthSession(properties);
    }

    private OAuth2Util.AuthSession initalizeAuthSession(Map<String, String> properties) {
        if (properties.containsKey("token")) {
            return new OAuth2Util.AuthSession(
                    Map.of(),
                    AuthConfig.builder()
                            .tokenType(OAuthTokenType.ACCESS_TOKEN.value())
                            .token(properties.get("token"))
                            .build()
            );
        }

        RESTClient restClient = HTTPClient.builder(Map.of())
                .uri(properties.get("oauth2-server-uri"))
                .build();

        if (properties.containsKey("credential")) {
            return OAuth2Util.AuthSession.fromCredential(
                    restClient,
                    ThreadPools.newScheduledPool( "service-admin-token-refresh", 1),
                    properties.get("credential"),
                    OAuth2Util.AuthSession.empty()
            );
        }

        for (String tokenType : TOKEN_PREFERENCE_ORDER) {
            if (properties.containsKey(tokenType)) {
                return OAuth2Util.AuthSession.fromTokenExchange(
                        restClient,
                        ThreadPools.newScheduledPool( "service-admin-token-refresh", 1),
                        properties.get(tokenType),
                        tokenType,
                        OAuth2Util.AuthSession.empty()
                );
            }
        }

        throw new IllegalArgumentException("Unable to construct authentication provider with the provided properties.");
    }

    public Map<String, String> getAuthHeaders() {
        return this.authSession.headers();
    }

}
