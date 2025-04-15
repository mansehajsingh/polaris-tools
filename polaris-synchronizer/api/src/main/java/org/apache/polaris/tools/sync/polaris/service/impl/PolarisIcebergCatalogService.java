package org.apache.polaris.tools.sync.polaris.service.impl;

import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.tools.sync.polaris.catalog.PolarisCatalog;
import org.apache.polaris.tools.sync.polaris.service.IcebergCatalogService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PolarisIcebergCatalogService implements IcebergCatalogService {

    private final PolarisCatalog catalog;

    public PolarisIcebergCatalogService(String uri, String catalogName, PrincipalWithCredentials migratorPrincipal) {
        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put("uri", uri);
        catalogProperties.put("warehouse", catalogName);

        String clientId = migratorPrincipal.getCredentials().getClientId();
        String clientSecret = migratorPrincipal.getCredentials().getClientSecret();
        catalogProperties.putIfAbsent(
                "credential", String.format("%s:%s", clientId, clientSecret));
        catalogProperties.putIfAbsent("scope", "PRINCIPAL_ROLE:ALL");

        this.catalog = (PolarisCatalog) CatalogUtil.loadCatalog(
                PolarisCatalog.class.getName(),
                "SOURCE_CATALOG_REST_" + catalogName,
                catalogProperties,
                null
        );
    }

    @Override
    public List<Namespace> listNamespaces(Namespace parentNamespace) {
        return this.catalog.listNamespaces(parentNamespace);
    }

    /**
     * List all namespaces in hierarchy underneath a particular namespace in addition to all
     * immediate children.
     * @param parentNamespace the namespace to search for child namespaces under
     * @return all child namespaces in hierarchy
     */
    private List<Namespace> listAllChildNamespaces(Namespace parentNamespace) {
        List<Namespace> immediateChildren = this.listNamespaces(parentNamespace);

        List<Namespace> allChildNamespaces = new ArrayList<>(immediateChildren);

        for (Namespace childNamespace : immediateChildren) {
            allChildNamespaces.addAll(this.listAllChildNamespaces(childNamespace));
        }

        return allChildNamespaces;
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(Namespace namespace) {
        return this.catalog.loadNamespaceMetadata(namespace);
    }

    @Override
    public void createNamespace(Namespace namespace, Map<String, String> namespaceMetadata) {
        this.catalog.createNamespace(namespace, namespaceMetadata);
    }

    @Override
    public void setNamespaceProperties(Namespace namespace, Map<String, String> namespaceProperties) {
        this.catalog.setProperties(namespace, namespaceProperties);
    }

    @Override
    public void dropNamespaceCascade(Namespace namespace) {
        List<Namespace> allChildNamespaces = this.listAllChildNamespaces(namespace);

        List<TableIdentifier> tables = new ArrayList<>();

        for (Namespace childNamespace : allChildNamespaces) {
            tables.addAll(this.catalog.listTables(childNamespace));
        }

        if (!namespace.isEmpty()) {
            tables.addAll(this.catalog.listTables(namespace));
        }

        for (TableIdentifier tableIdentifier : tables) {
            this.catalog.dropTable(tableIdentifier);
        }

        // go over in reverse order of namespaces since we discover namespaces
        // in the parent -> child order, so we need to drop all children
        // before we can drop the parent
        for (Namespace childNamespace : allChildNamespaces.reversed()) {
            this.catalog.dropNamespace(childNamespace);
        }

        if (!namespace.isEmpty()) {
            this.catalog.dropNamespace(namespace);
        }
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
        return this.catalog.listTables(namespace);
    }

    @Override
    public Table loadTable(TableIdentifier tableIdentifier) {
        return this.catalog.loadTable(tableIdentifier);
    }

    public Table loadTable(TableIdentifier tableIdentifier, String etag) {
        return this.catalog.loadTable(tableIdentifier, etag);
    }

    @Override
    public void registerTable(TableIdentifier tableIdentifier, String metadataFileLocation) {
        this.catalog.registerTable(tableIdentifier, metadataFileLocation);
    }

    @Override
    public void dropTableWithoutPurge(TableIdentifier tableIdentifier) {
        this.catalog.dropTable(tableIdentifier, false /* purge */);
    }

}
