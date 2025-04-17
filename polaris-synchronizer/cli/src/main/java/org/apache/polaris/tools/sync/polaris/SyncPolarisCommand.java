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
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.polaris.tools.sync.polaris.catalog.ETagManager;
import org.apache.polaris.tools.sync.polaris.planning.AccessControlAwarePlanner;
import org.apache.polaris.tools.sync.polaris.planning.ModificationAwarePlanner;
import org.apache.polaris.tools.sync.polaris.planning.SourceParitySynchronizationPlanner;
import org.apache.polaris.tools.sync.polaris.planning.SynchronizationPlanner;
import org.apache.polaris.tools.sync.polaris.service.PolarisService;
import org.apache.polaris.tools.sync.polaris.service.impl.PolarisApiService;
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
          names = {"--source-properties"},
          required = true,
          description = "Properties to initialize Polaris entity source." +
                  "\nProperties:" +
                  "\n\t- base-url: the base url of the Polaris instance (eg. http://localhost:8181)" +
                  "\n\t- bearer-token: the bearer token to authenticate against the Polaris instance with. Must " +
                  "be provided if any of oauth2-server-uri, client-id, client-secret, or scope are not provided." +
                  "\n\t- oauth2-server-uri: the uri of the OAuth2 server to authenticate to. (eg. http://localhost:8181/api/catalog/v1/oauth/tokens)" +
                  "\n\t- client-id: the client id belonging to a service admin to authenticate with" +
                  "\n\t- client-secret: the client secret belong to a service admin to authenticate with" +
                  "\n\t- scope: the scope to authenticate with for the service_admin (eg. PRINCIPAL_ROLE:ALL)" +
                  "\nOmnipotent Principal Properties:" +
                  "\n\t- omnipotent-principal-name: the name of the omnipotent principal created using create-omnipotent-principal on the source Polaris" +
                  "\n\t- omnipotent-principal-client-id: the client id of the omnipotent principal created using create-omnipotent-principal on the source Polaris" +
                  "\n\t- omnipotent-principal-client-secret: the client secret of the omnipotent principal created using create-omnipotent-principal on the source Polaris" +
                  "\n\t- omnipotent-principal-oauth2-server-uri: (default: /v1/oauth/tokens endpoint for provided Polaris base-url) "
                    + "the OAuth2 server to use to authenticate the omnipotent-principal for Iceberg catalog access"
  )
  private Map<String, String> sourceProperties;

  @CommandLine.Option(
          names = {"--target-properties"},
          required = true,
          description = "Properties to initialize Polaris entity target." +
                  "\nProperties:" +
                  "\n\t- base-url: the base url of the Polaris instance (eg. http://localhost:8181)" +
                  "\n\t- bearer-token: the bearer token to authenticate against the Polaris instance with. Must " +
                  "be provided if any of oauth2-server-uri, client-id, client-secret, or scope are not provided." +
                  "\n\t- oauth2-server-uri: the uri of the OAuth2 server to authenticate to. (eg. http://localhost:8181/api/catalog/v1/oauth/tokens)" +
                  "\n\t- client-id: the client id belonging to a service admin to authenticate with" +
                  "\n\t- client-secret: the client secret belong to a service admin to authenticate with" +
                  "\n\t- scope: the scope to authenticate with for the service_admin (eg. PRINCIPAL_ROLE:ALL)" +
                  "\nOmnipotent Principal Properties:" +
                  "\n\t- omnipotent-principal-name: the name of the omnipotent principal created using create-omnipotent-principal on the target Polaris" +
                  "\n\t- omnipotent-principal-client-id: the client id of the omnipotent principal created using create-omnipotent-principal on the target Polaris" +
                  "\n\t- omnipotent-principal-client-secret: the client secret of the omnipotent principal created using create-omnipotent-principal on the target Polaris" +
                  "\n\t- omnipotent-principal-oauth2-server-uri: (default: /v1/oauth/tokens endpoint for provided Polaris base-url) "
                  + "the OAuth2 server to use to retrieve a bearer token for the omnipotent-principal"
  )
  private Map<String, String> targetProperties;

  @CommandLine.Option(
          names = {"--etag-storage-type"},
          defaultValue = "NONE",
          description = "One of { NONE, FILE, CUSTOM }. Default: NONE. The storage manager to use for storing ETags."
  )
  private ETagManagerFactory.Type etagManagerType;

  @CommandLine.Option(
          names = {"--etag-storage-properties"},
          description = "Properties to initialize ETag storage." +
                  "\nFor type FILE:" +
                  "\n\t- " + CsvETagManager.CSV_FILE_PROPERTY + ": The CSV file to read ETags from and write ETags to." +
                  "\nFor type CUSTOM:" +
                  "\n\t- " + ETagManagerFactory.CUSTOM_CLASS_NAME_PROPERTY+ ": The classname for the custom ETagManager implementation."
  )
  private Map<String, String> etagManagerProperties;

  @CommandLine.Option(
          names = {"--sync-principals"},
          description = "Enable synchronization of principals across the source and target, and assign them to " +
                  "the appropriate principal roles. WARNING: Principal client-id and client-secret will be reset on " +
                  "the target Polaris instance, and the new credentials for the principals created on the target will " +
                  "be logged to stdout."
  )
  private boolean shouldSyncPrincipals;

  @CommandLine.Option(
          names = {"--halt-on-failure"},
          description = "Hard fail and stop the synchronization when an error occurs."
  )
  private boolean haltOnFailure;

  @Override
  public Integer call() throws Exception {
    SynchronizationPlanner sourceParityPlanner = new SourceParitySynchronizationPlanner();
    SynchronizationPlanner modificationAwareSourceParityPlanner = new ModificationAwarePlanner(sourceParityPlanner);
    SynchronizationPlanner accessControlAwarePlanner = new AccessControlAwarePlanner(modificationAwareSourceParityPlanner);

    // auto generate omnipotent principals with write access on the target, read only access on source
    sourceProperties.put(PolarisApiService.ICEBERG_WRITE_ACCESS_PROPERTY, Boolean.toString(false));
    targetProperties.put(PolarisApiService.ICEBERG_WRITE_ACCESS_PROPERTY, Boolean.toString(true));

    PolarisService source =
            PolarisServiceFactory.createPolarisService(PolarisServiceFactory.ServiceType.API, sourceProperties);
    PolarisService target =
            PolarisServiceFactory.createPolarisService(PolarisServiceFactory.ServiceType.API, targetProperties);

    ETagManager etagService = ETagManagerFactory.createETagManager(etagManagerType, etagManagerProperties);

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
            haltOnFailure,
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
