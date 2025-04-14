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
import java.util.concurrent.Callable;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.tools.sync.polaris.catalog.ETagManager;
import org.apache.polaris.tools.sync.polaris.catalog.NoOpETagManager;
import org.apache.polaris.tools.sync.polaris.options.SourceOmniPotentPrincipalOptions;
import org.apache.polaris.tools.sync.polaris.options.SourcePolarisOptions;
import org.apache.polaris.tools.sync.polaris.options.TargetOmnipotentPrincipal;
import org.apache.polaris.tools.sync.polaris.options.TargetPolarisOptions;
import org.apache.polaris.tools.sync.polaris.planning.AccessControlAwarePlanner;
import org.apache.polaris.tools.sync.polaris.planning.CatalogFilterPlanner;
import org.apache.polaris.tools.sync.polaris.planning.ModificationAwarePlanner;
import org.apache.polaris.tools.sync.polaris.planning.SourceParitySynchronizationPlanner;
import org.apache.polaris.tools.sync.polaris.planning.SynchronizationPlanner;
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

  @CommandLine.ArgGroup(
      exclusive = false,
      multiplicity = "1",
      heading = "Source Polaris options: %n")
  private SourcePolarisOptions sourcePolarisOptions;

  @CommandLine.ArgGroup(
      exclusive = false,
      multiplicity = "1",
      heading = "Target Polaris options: %n")
  private TargetPolarisOptions targetPolarisOptions;

  @CommandLine.ArgGroup(
      exclusive = false,
      multiplicity = "1",
      heading = "Source Polaris Omnipotent Principal Options: %n")
  private SourceOmniPotentPrincipalOptions sourceOmniPotentPrincipalOptions;

  @CommandLine.ArgGroup(
      exclusive = false,
      multiplicity = "1",
      heading = "Target Polaris Omnipotent Principal Options: %n")
  private TargetOmnipotentPrincipal targetOmniPotentPrincipalOptions;

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

  @CommandLine.Option(
          names = {"--catalog-name-regex"},
          description = "Filter catalogs by name matching the provided regular expression."
  )
  private String catalogNameRegex;

  @Override
  public Integer call() throws Exception {
    SynchronizationPlanner planner = new SourceParitySynchronizationPlanner();

    planner = new ModificationAwarePlanner(planner);

    if (catalogNameRegex != null) {
      planner = new CatalogFilterPlanner(catalogNameRegex, planner);
    }

    planner = new AccessControlAwarePlanner(planner);

    PolarisService source = sourcePolarisOptions.buildService();
    PolarisService target = targetPolarisOptions.buildService();

    PrincipalWithCredentials sourceOmnipotentPrincipal =
        sourceOmniPotentPrincipalOptions.buildPrincipalWithCredentials();
    PrincipalWithCredentials targetOmniPotentPrincipal =
        targetOmniPotentPrincipalOptions.buildPrincipalWithCredentials();

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
            planner,
            sourceOmnipotentPrincipal,
            targetOmniPotentPrincipal,
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
