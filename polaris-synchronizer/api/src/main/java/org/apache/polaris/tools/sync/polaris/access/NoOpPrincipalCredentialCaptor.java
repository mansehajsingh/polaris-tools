package org.apache.polaris.tools.sync.polaris.access;

import org.apache.polaris.core.admin.model.PrincipalWithCredentials;

/**
 * No-op implementation that does nothing with the principal credentials.
 */
public class NoOpPrincipalCredentialCaptor implements PrincipalCredentialCaptor {

    @Override
    public void storePrincipal(PrincipalWithCredentials principal) {
        // do nothing
    }

}
