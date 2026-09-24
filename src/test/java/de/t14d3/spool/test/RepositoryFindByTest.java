package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.repository.EntityRepository;
import de.t14d3.spool.test.entities.Author;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * T2.5: repository findBy ordering/paging and an existsById probe that never
 * loads the entity.
 */
public class RepositoryFindByTest {
    private EntityManager em;
    private QueryCountingConnection counting;
    private EntityRepository<Author> repository;

    @BeforeEach
    void setup() throws Exception {
        String url = "jdbc:h2:mem:repo_findby_" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1";

        EntityManager seed = EntityManager.create(url);
        try {
            seed.registerEntities(Author.class);
            seed.updateSchema();
            for (int i = 0; i < 10; i++) {
                seed.persist(new Author("Author " + i, "author" + i + "@example.com"));
            }
            seed.flush();
        } finally {
            seed.close();
        }

        counting = new QueryCountingConnection(DriverManager.getConnection(url));
        em = EntityManager.create(counting.getProxy());
        em.registerEntities(Author.class);
        repository = new EntityRepository<>(em, Author.class);
    }

    @AfterEach
    void teardown() {
        em.close();
    }

    @Test
    void findByAppliesOrderingLimitAndOffset() {
        List<Author> page = repository.findBy(Map.of(), "name DESC", 3, 2);
        assertEquals(List.of("Author 7", "Author 6", "Author 5"),
                page.stream().map(Author::getName).collect(Collectors.toList()));

        List<Author> filtered = repository.findBy(Map.of("email", "author9@example.com"), "name ASC", null, null);
        assertEquals(1, filtered.size());
        assertEquals("Author 9", filtered.get(0).getName());
    }

    @Test
    void findByRejectsUnknownOrderField() {
        assertThrows(IllegalArgumentException.class,
                () -> repository.findBy(Map.of(), "missing DESC", null, null));
    }

    @Test
    void existsByIdProbesWithSelectOne() {
        counting.reset();
        assertFalse(repository.existsById(9999L));
        assertEquals(1, counting.getPreparedStatements(),
                "existsById must issue a single SELECT 1 probe, not load the entity");

        List<Author> all = repository.findAll();
        Author managed = all.get(0);
        counting.reset();
        assertTrue(repository.existsById(managed.getId()), "managed entities short-circuit without SQL");
        assertEquals(0, counting.getPreparedStatements());
    }
}
