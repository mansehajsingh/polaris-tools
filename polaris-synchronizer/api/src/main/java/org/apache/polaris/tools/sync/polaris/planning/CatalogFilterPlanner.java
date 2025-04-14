package org.apache.polaris.tools.sync.polaris.planning;

import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.tools.sync.polaris.planning.plan.SynchronizationPlan;
import org.apache.polaris.tools.sync.polaris.planning.util.IdentifierIntersectionFilterUtil;

import java.util.List;
import java.util.regex.Pattern;

public class CatalogFilterPlanner extends DelegatedPlanner implements SynchronizationPlanner {

    private final Pattern nameRegex;

    public CatalogFilterPlanner(String nameRegex, SynchronizationPlanner delegate) {
        super(delegate);
        this.nameRegex = Pattern.compile(nameRegex);
    }

    @Override
    public SynchronizationPlan<Catalog> planCatalogSync(List<Catalog> catalogsOnSource, List<Catalog> catalogsOnTarget) {
        IdentifierIntersectionFilterUtil.FilterResult<Catalog> result =
                IdentifierIntersectionFilterUtil.filterOnCondition(
                        catalogsOnSource,
                        catalogsOnTarget,
                        Catalog::getName,
                        (c1, c2) -> nameRegex.matcher(c1.getName()).matches()
                );

        SynchronizationPlan<Catalog> delegatedPlan =
                this.delegate.planCatalogSync(result.sourceEntitiesFiltered(), result.targetEntitiesFiltered());

        for (Catalog catalog : result.sourceEntitiesFilteredOut()) {
            delegatedPlan.skipEntityAndSkipChildren(catalog);
        }

        return delegatedPlan;
    }

}
