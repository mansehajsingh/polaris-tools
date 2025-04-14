package org.apache.polaris.tools.sync.polaris.planning.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

public class IdentifierIntersectionFilterUtil {

    private IdentifierIntersectionFilterUtil() {}

    public record FilterResult<T>(List<T> sourceEntitiesFiltered,
                                  List<T> targetEntitiesFiltered,
                                  List<T> sourceEntitiesFilteredOut,
                                  List<T> targetEntitiesFilteredOut) {}

    public static <T> FilterResult<T> filterOnCondition(Collection<T> entitiesOnSource,
                                                        Collection<T> entitiesOnTarget,
                                                        Function<T, Object> identifierRetriever,
                                                        BiFunction<T, T, Boolean> filterCondition
    ) {
        Map<Object, T> entitiesByIdentifierSource = new HashMap<>();
        Map<Object, T> entitiesByIdentifierTarget = new HashMap<>();
        
        List<T> sourceEntitiesFilteredOut = new ArrayList<>();
        List<T> targetEntitiesFilteredOut = new ArrayList<>();
        
        entitiesOnSource.forEach(entity -> entitiesByIdentifierSource.put(identifierRetriever.apply(entity), entity));
        entitiesOnTarget.forEach(entity -> entitiesByIdentifierTarget.put(identifierRetriever.apply(entity), entity));
        
        for (T sourceEntity : entitiesOnSource) {
            Object sourceIdentifier = identifierRetriever.apply(sourceEntity);
            if (entitiesByIdentifierTarget.containsKey(sourceIdentifier)) {
                T targetEntity = entitiesByIdentifierTarget.get(sourceIdentifier);

                if (!filterCondition.apply(sourceEntity, targetEntity)) {
                    sourceEntitiesFilteredOut.add(sourceEntity);
                    targetEntitiesFilteredOut.add(targetEntity);
                    entitiesByIdentifierSource.remove(sourceIdentifier);
                    entitiesByIdentifierTarget.remove(sourceIdentifier);
                }
            }
        }

        return new FilterResult<>(
                entitiesByIdentifierSource.values().stream().toList(),
                entitiesByIdentifierTarget.values().stream().toList(),
                sourceEntitiesFilteredOut,
                targetEntitiesFilteredOut
        );
    }

}
