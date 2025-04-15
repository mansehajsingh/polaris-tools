package org.apache.polaris.tools.sync.polaris.service;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.List;
import java.util.Map;

/**
 * Wrapper around {@link org.apache.iceberg.catalog.Catalog} that exposes functionality
 * that uses multiple Iceberg operations. For example, cascading drops of namespaces.
 */
public interface IcebergCatalogService {

    // NAMESPACES
    List<Namespace> listNamespaces(Namespace parentNamespace);
    Map<String, String> loadNamespaceMetadata(Namespace namespace);
    void createNamespace(Namespace namespace, Map<String, String> namespaceMetadata);
    void setNamespaceProperties(Namespace namespace, Map<String, String> namespaceProperties);

    /**
     * Drop a namespace by first dropping all nested namespaces and tables underneath the namespace
     * hierarchy. The empty namespace will not be dropped.
     * @param namespace the namespace to drop
     */
    void dropNamespaceCascade(Namespace namespace);

    // TABLES
    List<TableIdentifier> listTables(Namespace namespace);
    Table loadTable(TableIdentifier tableIdentifier);
    void registerTable(TableIdentifier tableIdentifier, String metadataFileLocation);
    void dropTableWithoutPurge(TableIdentifier tableIdentifier);

}
