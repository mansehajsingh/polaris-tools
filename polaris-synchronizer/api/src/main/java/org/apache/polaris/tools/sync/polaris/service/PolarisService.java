package org.apache.polaris.tools.sync.polaris.service;

import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.Principal;
import org.apache.polaris.core.admin.model.PrincipalRole;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;

import java.util.List;
import java.util.Map;

/**
 * Generic wrapper for a Polaris entity store.
 */
public interface PolarisService {

    /**
     * Called to perform initializing tasks for a Polaris entity store.
     * @param properties the properties used to initialize the service
     * @throws Exception
     */
    void initialize(Map<String, String> properties) throws Exception;

    // PRINCIPALS
    List<Principal> listPrincipals();
    Principal getPrincipal(String principalName);
    PrincipalWithCredentials createPrincipal(Principal principal);
    void dropPrincipal(String principalName);

    // PRINCIPAL ROLES
    List<PrincipalRole> listPrincipalRoles();
    PrincipalRole getPrincipalRole(String principalRoleName);
    void createPrincipalRole(PrincipalRole principalRole);
    void dropPrincipalRole(String principalRoleName);

    // ASSIGNMENT OF PRINCIPAL ROLES TO PRINCIPALS
    List<PrincipalRole> listPrincipalRolesAssigned(String principalName);
    void assignPrincipalRole(String principalName, String principalRoleName);
    void revokePrincipalRole(String principalName, String principalRoleName);

    // CATALOGS
    List<Catalog> listCatalogs();
    Catalog getCatalog(String catalogName);
    void createCatalog(Catalog catalog);
    void dropCatalogCascade(String catalogName);

    // CATALOG ROLES
    List<CatalogRole> listCatalogRoles(String catalogName);
    CatalogRole getCatalogRole(String catalogName, String catalogRoleName);
    void createCatalogRole(String catalogName, CatalogRole catalogRole);
    void dropCatalogRole(String catalogName, String catalogRoleName);

    // ASSIGNMENT OF CATALOG ROLES TO CATALOGS
    List<PrincipalRole> listAssigneePrincipalRolesForCatalogRole(String catalogName, String catalogRoleName);
    void assignCatalogRole(String principalRoleName, String catalogName, String catalogRoleName);
    void revokeCatalogRole(String principalRoleName, String catalogName, String catalogRoleName);

    // GRANTS
    List<GrantResource> listGrants(String catalogName, String catalogRoleName);
    void addGrant(String catalogName, String catalogRoleName, GrantResource grant);
    void revokeGrant(String catalogName, String catalogRoleName, GrantResource grant);

    // ICEBERG
    IcebergCatalogService initializeIcebergCatalogService(String catalogName);

}
