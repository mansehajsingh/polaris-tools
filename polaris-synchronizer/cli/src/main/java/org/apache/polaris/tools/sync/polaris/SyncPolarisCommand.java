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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.polaris.tools.sync.polaris.catalog.ETagManager;
import org.apache.polaris.tools.sync.polaris.catalog.NoOpETagManager;
import org.apache.polaris.tools.sync.polaris.planning.AccessControlAwarePlanner;
import org.apache.polaris.tools.sync.polaris.planning.ModificationAwarePlanner;
import org.apache.polaris.tools.sync.polaris.planning.SourceParitySynchronizationPlanner;
import org.apache.polaris.tools.sync.polaris.planning.SynchronizationPlanner;
import org.apache.polaris.tools.sync.polaris.service.PolarisService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/**
 * Command to run the synchronization between a source and target Polaris instance.
 */
@CommandLine.Command(
    name = "sync-polaris",
    mixinStandardHelpOptions = true,
    sortOptions = false,
    description =
        "Idempotent synchronization of one Polaris instance to another. Entities will not be removed from the source Polaris instance.")
public class SyncPolarisCommand implements Callable<Integer> {

  private final Logger consoleLog = LoggerFactory.getLogger("console-log");

  @CommandLine.Option(
          names = {"--source-type"},
          required = true,
          description = "The type of the Polaris entity source. One of { API }"
  )
  private PolarisServiceFactory.ServiceType sourceType;

  @CommandLine.Option(
          names = {"--source-properties"},
          required = true,
          description = "Properties to initialize Polaris entity source." +
                  "\nProperties (source-type=API):" +
                  "\n\t- base-url: the base url of the Polaris instance (eg. http://localhost:8181)" +
                  "\n\t- bearer-token: the bearer token to authenticate against the Polaris instance with. Must " +
                  "be provided if any of oauth2-server-uri, client-id, client-secret, or scope are not provided." +
                  "\n\t- oauth2-server-uri: the uri of the OAuth2 server to authenticate to. (eg. http://localhost:8181/api/catalog/v1/oauth/tokens)" +
                  "\n\t- client-id: the client id belonging to a service admin to authenticate with" +
                  "\n\t- client-secret: the client secret belong to a service admin to authenticate with" +
                  "\n\t- scope: the scope to authenticate with for the service_admin (eg. PRINCIPAL_ROLE:ALL)"
  )
  private Map<String, String> sourceProperties;

  @CommandLine.Option(
          names = {"--target-type"},
          required = true,
          description = "The type of the Polaris entity target. One of { API }"
  )
  private PolarisServiceFactory.ServiceType targetType;

  @CommandLine.Option(
          names = {"--target-properties"},
          required = true,
          description = "Properties to initialize Polaris entity target." +
                  "\nProperties (target-type=API):" +
                  "\n\t- base-url: the base url of the Polaris instance (eg. http://localhost:8181)" +
                  "\n\t- bearer-token: the bearer token to authenticate against the Polaris instance with. Must " +
                  "be provided if any of oauth2-server-uri, client-id, client-secret, or scope are not provided." +
                  "\n\t- oauth2-server-uri: the uri of the OAuth2 server to authenticate to. (eg. http://localhost:8181/api/catalog/v1/oauth/tokens)" +
                  "\n\t- client-id: the client id belonging to a service admin to authenticate with" +
                  "\n\t- client-secret: the client secret belong to a service admin to authenticate with" +
                  "\n\t- scope: the scope to authenticate with for the service_admin (eg. PRINCIPAL_ROLE:ALL)"
  )
  private Map<String, String> targetProperties;

  @CommandLine.Option(
      names = {"--etag-file"},
      description = "The file path of the file to retrieve and store table ETags from.")
  private String etagFilePath;

  @CommandLine.Option(
          names = {"--sync-principals"},
          description = "Enable synchronization of principals across the source and target, and assign them to " +
                  "the appropriate principal roles. WARNING: Principal client-id and client-secret will be reset on " +
                  "the target Polaris instance, and the new credentials for the principals created on the target will " +
                  "be logged to stdout."
  )
  private boolean shouldSyncPrincipals;

  @Override
  public Integer call() throws Exception {
    SynchronizationPlanner sourceParityPlanner = new SourceParitySynchronizationPlanner();
    SynchronizationPlanner modificationAwareSourceParityPlanner = new ModificationAwarePlanner(sourceParityPlanner);
    SynchronizationPlanner accessControlAwarePlanner = new AccessControlAwarePlanner(modificationAwareSourceParityPlanner);


    PolarisService source = PolarisServiceFactory.createPolarisService(
            sourceType, PolarisServiceFactory.EndpointType.SOURCE, sourceProperties);
    PolarisService target = PolarisServiceFactory.createPolarisService(
            sourceType, PolarisServiceFactory.EndpointType.TARGET, targetProperties);

    ETagManager etagService;

    if (etagFilePath != null) {
      File etagFile = new File(etagFilePath);
      etagService = new CsvETagManager(etagFile);
    } else {
      etagService = new NoOpETagManager();
    }

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  if (etagService instanceof Closeable closableETagService) {
                    try {
                      closableETagService.close();
                    } catch (IOException e) {
                      throw new RuntimeException(e);
                    }
                  }
                }));

    PolarisSynchronizer synchronizer =
        new PolarisSynchronizer(
            consoleLog,
            accessControlAwarePlanner,
            source,
            target,
            etagService);
    synchronizer.syncPrincipalRoles();
    if (shouldSyncPrincipals) {
      consoleLog.warn("Principal migration will reset credentials on the target Polaris instance. " +
              "Principal migration will log the new target Principal credentials to stdout.");
      synchronizer.syncPrincipals();
    }
    synchronizer.syncCatalogs();

    return 0;
  }
}
