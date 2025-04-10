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
package org.apache.polaris.tools.sync.polaris.options;

import java.util.Map;
import picocli.CommandLine;

public class TargetPolarisOptions extends BasePolarisOptions {

  @Override
  public String getServiceName() {
    return "target";
  }

  @CommandLine.Option(
      names = "--target-" + BASE_URL,
      required = true,
      description = "The base url of the Polaris instance. Example: http://localhost:8181/polaris.")
  @Override
  public void setBaseUrl(String baseUrl) {
    this.baseUrl = baseUrl;
  }

  @CommandLine.Option(
      names = "--target-" + AUTHENTICATION_PROPERTIES,
      required = true,
      description = "The authentication configuration for the service admin on the target Polaris.")
  @Override
  public void setAuthenticationProperties(Map<String, String> authenticationProperties) {
    this.authenticationProperties = authenticationProperties;
  }
}
