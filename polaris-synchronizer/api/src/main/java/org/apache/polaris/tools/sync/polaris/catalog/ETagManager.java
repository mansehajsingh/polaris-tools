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
package org.apache.polaris.tools.sync.polaris.catalog;

import org.apache.iceberg.catalog.TableIdentifier;

import java.util.Map;

/**
 * Generic interface to provide and store ETags for tables within catalogs. This allows the storage
 * of the ETag to be completely independent from the tool.
 */
public interface ETagManager {

  /**
   * Used to initialize the instance for use. Should be called prior to
   * calling any methods.
   * @param properties properties to configure instance with
   */
  void initialize(Map<String, String> properties);

  /**
   * Retrieves the ETag for the table.
   *
   * @param catalogName the catalog the table is in
   * @param tableIdentifier the table identifier
   * @return The ETag for the last known metadata for the table
   */
  String getETag(String catalogName, TableIdentifier tableIdentifier);

  /**
   * After table loading, stores the fetched ETag.
   *
   * @param catalogName the catalog the table is in
   * @param tableIdentifier the table identifier
   * @param etag the ETag that was provided by the Iceberg REST api
   */
  void storeETag(String catalogName, TableIdentifier tableIdentifier, String etag);
}
