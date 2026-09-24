package de.t14d3.spool.mapping;

import de.t14d3.spool.annotations.PostPersist;
import de.t14d3.spool.annotations.PostRemove;
import de.t14d3.spool.annotations.PostUpdate;
import de.t14d3.spool.annotations.PreLoad;
import de.t14d3.spool.annotations.PrePersist;
import de.t14d3.spool.annotations.PreRemove;
import de.t14d3.spool.annotations.PreUpdate;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Lifecycle callback metadata for an entity class.
 * <p>
 * Scans the class hierarchy for no-argument methods annotated with the
 * lifecycle annotations, caches the result per class and invokes them at the
 * appropriate persistence moments. Superclass callbacks run before subclass
 * callbacks. When a callback method is overridden, the overriding declaration
 * must carry the annotation as well; otherwise the superclass declaration is
 * ignored (the override replaces it).
 */
public final class EntityCallbacks {

    /**
     * The lifecycle moments a callback can be attached to.
     */
    public enum Phase {
        PRE_PERSIST,
        POST_PERSIST,
        PRE_UPDATE,
        POST_UPDATE,
        PRE_REMOVE,
        POST_REMOVE,
        PRE_LOAD
    }

    private static final Map<Class<?>, EntityCallbacks> CACHE = new ConcurrentHashMap<>();

    private final Map<Phase, List<Method>> methods;

    private EntityCallbacks(Class<?> entityClass) {
        Map<Phase, List<Method>> collected = new EnumMap<>(Phase.class);
        for (Phase phase : Phase.values()) {
            collected.put(phase, new ArrayList<>());
        }
        collect(entityClass, collected);

        Map<Phase, List<Method>> frozen = new EnumMap<>(Phase.class);
        for (Map.Entry<Phase, List<Method>> entry : collected.entrySet()) {
            frozen.put(entry.getKey(), List.copyOf(entry.getValue()));
        }
        this.methods = Collections.unmodifiableMap(frozen);
    }

    /**
     * Get (or build) the callback metadata for an entity class.
     */
    public static EntityCallbacks of(Class<?> entityClass) {
        return CACHE.computeIfAbsent(entityClass, EntityCallbacks::new);
    }

    private static void collect(Class<?> entityClass, Map<Phase, List<Method>> out) {
        // Walk from the entity class upward so an overriding subclass method
        // takes precedence over the superclass declaration it replaces.
        Map<String, Method> bySignature = new java.util.LinkedHashMap<>();
        Map<Class<?>, Integer> depth = new java.util.HashMap<>();
        int level = 0;
        for (Class<?> current = entityClass; current != null && current != Object.class; current = current.getSuperclass()) {
            depth.put(current, level++);
            for (Method method : current.getDeclaredMethods()) {
                Phase phase = phaseOf(method);
                if (phase == null) {
                    continue;
                }
                if (method.getParameterCount() != 0) {
                    throw new IllegalStateException(
                            "Lifecycle callback " + current.getName() + "." + method.getName()
                                    + " must not declare parameters");
                }
                method.setAccessible(true);
                bySignature.putIfAbsent(method.getName(), method);
            }
        }

        List<Method> ordered = new ArrayList<>(bySignature.values());
        // Superclass callbacks run before subclass callbacks.
        ordered.sort(java.util.Comparator.comparingInt(m -> -depth.getOrDefault(m.getDeclaringClass(), 0)));

        for (Method method : ordered) {
            out.get(phaseOf(method)).add(method);
        }
    }

    private static Phase phaseOf(Method method) {
        if (method.isAnnotationPresent(PrePersist.class)) return Phase.PRE_PERSIST;
        if (method.isAnnotationPresent(PostPersist.class)) return Phase.POST_PERSIST;
        if (method.isAnnotationPresent(PreUpdate.class)) return Phase.PRE_UPDATE;
        if (method.isAnnotationPresent(PostUpdate.class)) return Phase.POST_UPDATE;
        if (method.isAnnotationPresent(PreRemove.class)) return Phase.PRE_REMOVE;
        if (method.isAnnotationPresent(PostRemove.class)) return Phase.POST_REMOVE;
        if (method.isAnnotationPresent(PreLoad.class)) return Phase.PRE_LOAD;
        return null;
    }

    /**
     * Invoke all callbacks registered for the given phase. A throwing callback
     * aborts the operation (and therefore the surrounding flush) and its cause
     * is rethrown wrapped in a {@link RuntimeException}.
     */
    public void invoke(Phase phase, Object entity) {
        List<Method> phaseMethods = methods.get(phase);
        if (phaseMethods == null || phaseMethods.isEmpty()) {
            return;
        }
        for (Method method : phaseMethods) {
            try {
                method.invoke(entity);
            } catch (InvocationTargetException e) {
                Throwable cause = e.getCause() != null ? e.getCause() : e;
                throw new RuntimeException(
                        "Lifecycle callback " + phase + " failed on " + entity.getClass().getName()
                                + "#" + method.getName(), cause);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(
                        "Cannot access lifecycle callback " + entity.getClass().getName() + "#" + method.getName(), e);
            }
        }
    }
}
