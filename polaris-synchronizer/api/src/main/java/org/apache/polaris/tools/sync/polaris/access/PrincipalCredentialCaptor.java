package org.apache.polaris.tools.sync.polaris.access;

import org.apache.polaris.core.admin.model.PrincipalWithCredentials;

/**
 * Captures principal credentials at the time of a principal's
 * creation/overwrite on the target Polaris instance.
 */
public interface PrincipalCredentialCaptor {

    /**
     * Store the principal with associated credentials at the time of its creation on the target instance.
     * @param principal the principal being created/overwritten on the target Polaris instance
     */
    void storePrincipal(PrincipalWithCredentials principal);

}
