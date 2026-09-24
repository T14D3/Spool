package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.test.entities.TypeMatrix;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TypeMappingRoundTripTest {
    private EntityManager em;

    @BeforeEach
    void setup() {
        em = EntityManager.create("jdbc:h2:mem:type_roundtrip_test;DB_CLOSE_DELAY=-1");
        em.getExecutor().execute("DROP TABLE IF EXISTS type_matrix", java.util.List.of());
        em.registerEntities(TypeMatrix.class);
        em.updateSchema();
    }

    @AfterEach
    void teardown() {
        em.close();
    }

    @Test
    void allSupportedTypesRoundTripThroughTheDatabase() {
        TypeMatrix entity = new TypeMatrix();
        entity.setTextValue("hello");
        entity.setIntValue(42);
        entity.setLongValue(9_000_000_000L);
        entity.setDoubleValue(1.5d);
        entity.setFlag(true);
        entity.setAmount(new BigDecimal("1234567890.12345"));
        entity.setLocalDate(LocalDate.of(2026, 1, 2));
        entity.setLocalDateTime(LocalDateTime.of(2026, 1, 2, 3, 4, 5, 123_456_000));
        entity.setInstantValue(Instant.parse("2026-01-02T03:04:05.123456Z"));
        entity.setOffsetDateTime(OffsetDateTime.parse("2026-01-02T03:04:05.123456Z"));
        entity.setUuidValue(UUID.fromString("11111111-2222-3333-4444-555555555555"));
        entity.setBlobValue(new byte[]{1, 2, 3, 4});
        entity.setStatus(TypeMatrix.Status.ARCHIVED);

        em.persist(entity);
        em.flush();

        // Force a database read instead of an identity-map hit.
        em.clear();
        TypeMatrix loaded = em.find(TypeMatrix.class, entity.getId());
        assertNotNull(loaded);

        assertEquals("hello", loaded.getTextValue());
        assertEquals(42, loaded.getIntValue());
        assertEquals(9_000_000_000L, loaded.getLongValue());
        assertEquals(1.5d, loaded.getDoubleValue());
        assertEquals(true, loaded.getFlag());
        assertEquals(0, new BigDecimal("1234567890.12345").compareTo(loaded.getAmount()));
        assertEquals(LocalDate.of(2026, 1, 2), loaded.getLocalDate());
        assertEquals(LocalDateTime.of(2026, 1, 2, 3, 4, 5, 123_456_000), loaded.getLocalDateTime());
        assertEquals(Instant.parse("2026-01-02T03:04:05.123456Z"), loaded.getInstantValue());
        assertEquals(OffsetDateTime.parse("2026-01-02T03:04:05.123456Z"), loaded.getOffsetDateTime());
        assertEquals(UUID.fromString("11111111-2222-3333-4444-555555555555"), loaded.getUuidValue());
        assertArrayEquals(new byte[]{1, 2, 3, 4}, loaded.getBlobValue());
        assertEquals(TypeMatrix.Status.ARCHIVED, loaded.getStatus());
    }

    @Test
    void schemaIsStableAfterCreation() {
        // A second updateSchema must not try to modify the generated columns
        // (DECIMAL gets an explicit precision/scale, which the introspector
        // must consider equivalent).
        assertEquals(0, em.updateSchema());
    }

    @Test
    void nullValuesRoundTrip() {
        TypeMatrix entity = new TypeMatrix();
        em.persist(entity);
        em.flush();

        em.clear();
        TypeMatrix loaded = em.find(TypeMatrix.class, entity.getId());
        assertNotNull(loaded);
        assertEquals(null, loaded.getAmount());
        assertEquals(null, loaded.getInstantValue());
        assertEquals(null, loaded.getStatus());
    }

    @Test
    void bigDecimalPrecisionIsNotLostThroughDatabaseRoundTrip() {
        TypeMatrix entity = new TypeMatrix();
        // A double cannot represent this value exactly; the old mapping went
        // through doubleValue() and would corrupt it.
        entity.setAmount(new BigDecimal("9007199254740993.0001"));
        em.persist(entity);
        em.flush();

        em.clear();
        TypeMatrix loaded = em.find(TypeMatrix.class, entity.getId());
        assertEquals(0, new BigDecimal("9007199254740993.0001").compareTo(loaded.getAmount()));
    }
}
