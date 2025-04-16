package org.apache.polaris.tools.sync.polaris.planning;

import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.tools.sync.polaris.planning.plan.SynchronizationPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NameFilterPlanner extends DelegatedPlanner implements SynchronizationPlanner {

    final String sourceRegexMatchingPattern;
    final String targetRegexMatchingPattern;
    public NameFilterPlanner(SynchronizationPlanner delegate, Map<String, String> sourceProperties, Map<String, String> targetProperties) {
        super(delegate);
        this.sourceRegexMatchingPattern = sourceProperties.get("catalog-filter") != null ? sourceProperties.get("catalog-filter") : "*";
        this.targetRegexMatchingPattern = targetProperties.get("catalog-filter") != null ? targetProperties.get("catalog-filter") : "*";
    }


    @Override
    public SynchronizationPlan<Catalog> planCatalogSync(List<Catalog> catalogsOnSource, List<Catalog> catalogsOnTarget) {
        List<Catalog> filteredSourceCatalogs = new ArrayList<>();
        List<Catalog> skippedSourceCatalogs = new ArrayList<>();
        List<Catalog> filteredTargetCatalogs = new ArrayList<>();
        for (Catalog catalog : catalogsOnSource) {
            if (catalog.getName().matches(sourceRegexMatchingPattern)) {
                filteredSourceCatalogs.add(catalog);
            } else {
                skippedSourceCatalogs.add(catalog);
            }
        }

        for (Catalog catalog : catalogsOnTarget) {
            if (catalog.getName().matches(targetRegexMatchingPattern)) {
                filteredTargetCatalogs.add(catalog);
            }
        }

        SynchronizationPlan<Catalog> delegatedPlan = delegate.planCatalogSync(filteredSourceCatalogs, filteredTargetCatalogs);
        skippedSourceCatalogs.forEach(delegatedPlan::skipEntityAndSkipChildren);
        return delegatedPlan;
    }
}
