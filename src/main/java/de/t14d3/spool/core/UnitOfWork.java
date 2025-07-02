package de.t14d3.spool.core;

import de.t14d3.spool.mapping.EntityMetadata;
import java.util.*;

public class UnitOfWork {
    private final Set<Object> newEntities     = new HashSet<>();
    private final Set<Object> dirtyEntities   = new HashSet<>();
    private final Set<Object> removedEntities = new HashSet<>();

    public void registerNew(Object entity) {
        newEntities.add(entity);
    }

    public void registerDirty(Object entity) {
        if (!newEntities.contains(entity)) {
            dirtyEntities.add(entity);
        }
    }

    public void registerRemoved(Object entity) {
        // if it was scheduled for insert, cancel that
        newEntities.remove(entity);
        dirtyEntities.remove(entity);
        removedEntities.add(entity);
    }

    public void commit(Persister executor) {
        for (Object e : newEntities) {
            EntityMetadata md = EntityMetadata.of(e.getClass());
            executor.insert(e, md);
        }
        for (Object e : dirtyEntities) {
            EntityMetadata md = EntityMetadata.of(e.getClass());
            executor.update(e, md);
        }
        for (Object e : removedEntities) {
            EntityMetadata md = EntityMetadata.of(e.getClass());
            executor.delete(e, md);
        }

        newEntities.clear();
        dirtyEntities.clear();
        removedEntities.clear();
    }
}
