package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.test.entities.Company;
import de.t14d3.spool.test.entities.Department;
import de.t14d3.spool.test.entities.Employee;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unidirectional {@code @OneToMany} with {@code @JoinColumn}: the FK column on
 * the target table is created by the schema diff and kept in sync by flush()
 * for lazy, eager and replaced (untracked) collections.
 */
public class UnidirectionalOneToManyTest {
    private static final String URL = "jdbc:h2:mem:unidirectional_o2m;DB_CLOSE_DELAY=-1";

    private EntityManager openSchema() {
        EntityManager em = EntityManager.create(URL);
        em.getExecutor().execute("DROP TABLE IF EXISTS employees", List.of());
        em.getExecutor().execute("DROP TABLE IF EXISTS departments", List.of());
        em.getExecutor().execute("DROP TABLE IF EXISTS companies", List.of());
        em.registerEntities(Department.class, Employee.class, Company.class);
        em.updateSchema();
        return em;
    }

    @Test
    void lazyCollectionManagesForeignKeyOnPersistAndRemove() throws Exception {
        EntityManager em = openSchema();
        Long departmentId;
        Long removedId;
        Long keptId;
        try {
            Department department = new Department("Engineering");
            Employee removed = new Employee("ada");
            Employee kept = new Employee("bob");
            department.getEmployees().add(removed);
            department.getEmployees().add(kept);

            em.persist(department);
            em.flush();

            departmentId = department.getId();
            removedId = removed.getId();
            keptId = kept.getId();
            assertEquals(departmentId, fkOf(em, "department_id", removedId));
            assertEquals(departmentId, fkOf(em, "department_id", keptId));

            department.getEmployees().remove(removed);
            em.flush();

            assertNull(fkOf(em, "department_id", removedId));
            assertEquals(departmentId, fkOf(em, "department_id", keptId));
        } finally {
            em.close();
        }

        EntityManager reader = EntityManager.create(URL);
        try {
            reader.registerEntities(Department.class, Employee.class, Company.class);
            Department loaded = reader.find(Department.class, departmentId);
            assertNotNull(loaded);
            assertEquals(1, loaded.getEmployees().size(), "lazy unidirectional collection must load children by FK");
            assertEquals(keptId, loaded.getEmployees().get(0).getId());
        } finally {
            reader.close();
        }
    }

    @Test
    void movingChildBetweenOwnersUpdatesBothForeignKeys() throws Exception {
        EntityManager em = openSchema();
        try {
            Department from = new Department("From");
            Department to = new Department("To");
            Employee employee = new Employee("carol");
            from.getEmployees().add(employee);

            em.persist(from);
            em.persist(to);
            em.flush();
            assertEquals(from.getId(), fkOf(em, "department_id", employee.getId()));

            // Plain collections on freshly inserted entities are untracked;
            // flush must still diff and move the FK.
            from.getEmployees().remove(employee);
            to.getEmployees().add(employee);
            em.flush();

            assertEquals(to.getId(), fkOf(em, "department_id", employee.getId()));
        } finally {
            em.close();
        }
    }

    @Test
    void deletingOwnerNullsChildForeignKeyWithoutDeletingChild() throws Exception {
        EntityManager em = openSchema();
        try {
            Department department = new Department("Temp");
            Employee employee = new Employee("dave");
            department.getEmployees().add(employee);
            em.persist(department);
            em.flush();
            Long employeeId = employee.getId();
            assertEquals(department.getId(), fkOf(em, "department_id", employeeId));

            em.remove(department);
            em.flush();

            assertNull(fkOf(em, "department_id", employeeId), "FK must be nulled before the owner row is deleted");
            assertEquals(1, countEmployee(em, employeeId), "child row must survive an owner delete without cascade REMOVE");
        } finally {
            em.close();
        }
    }

    @Test
    void eagerCollectionIsHydratedAndTracksNewChildren() throws Exception {
        EntityManager em = openSchema();
        Long companyId;
        try {
            Company company = new Company("Acme");
            company.getEmployees().add(new Employee("erin"));
            company.getEmployees().add(new Employee("frank"));
            em.persist(company);
            em.flush();
            companyId = company.getId();
        } finally {
            em.close();
        }

        EntityManager reader = EntityManager.create(URL);
        try {
            reader.registerEntities(Department.class, Employee.class, Company.class);
            Company loaded = reader.find(Company.class, companyId);
            assertNotNull(loaded);
            assertEquals(2, loaded.getEmployees().size());

            Employee grace = new Employee("grace");
            reader.persist(grace);
            loaded.getEmployees().add(grace);
            reader.flush();
            assertNotNull(grace.getId());
            assertEquals(companyId, fkOf(reader, "company_id", grace.getId()));
        } finally {
            reader.close();
        }
    }

    private Long fkOf(EntityManager em, String column, Long employeeId) throws Exception {
        String sql = "SELECT " + column + " FROM employees WHERE id = ?";
        try (PreparedStatement ps = em.getExecutor().getConnection().prepareStatement(sql)) {
            ps.setLong(1, employeeId);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                long value = rs.getLong(1);
                return rs.wasNull() ? null : value;
            }
        }
    }

    private int countEmployee(EntityManager em, Long employeeId) throws Exception {
        try (PreparedStatement ps = em.getExecutor().getConnection()
                .prepareStatement("SELECT COUNT(*) FROM employees WHERE id = ?")) {
            ps.setLong(1, employeeId);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return rs.getInt(1);
            }
        }
    }
}
