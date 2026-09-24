package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.test.entities.Company;
import de.t14d3.spool.test.entities.Department;
import de.t14d3.spool.test.entities.Employee;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T2.2: EAGER collection associations are batch-loaded for a whole result set
 * (one pair query plus one IN load), not one query per parent.
 */
public class EagerCollectionBatchTest {

    @Test
    void findAllBatchesEagerCollections() throws Exception {
        String dbName = "eager_collection_batch_" + UUID.randomUUID().toString().replace("-", "");
        String url = "jdbc:h2:mem:" + dbName + ";DB_CLOSE_DELAY=-1";

        EntityManager seed = EntityManager.create(url);
        try {
            seed.registerEntities(Company.class, Employee.class, Department.class);
            seed.updateSchema();
            for (int companyIndex = 0; companyIndex < 20; companyIndex++) {
                Company company = new Company("Company " + companyIndex);
                for (int employeeIndex = 0; employeeIndex < 3; employeeIndex++) {
                    Employee employee = new Employee("Employee " + companyIndex + "-" + employeeIndex);
                    company.getEmployees().add(employee);
                }
                seed.persist(company);
            }
            seed.flush();
        } finally {
            seed.close();
        }

        QueryCountingConnection counting = new QueryCountingConnection(DriverManager.getConnection(url));
        EntityManager em = EntityManager.create(counting.getProxy());
        try {
            em.registerEntities(Company.class, Employee.class, Department.class);

            counting.reset();
            List<Company> companies = em.findAll(Company.class);

            assertEquals(20, companies.size());
            for (Company company : companies) {
                assertEquals(3, company.getEmployees().size(),
                        "eager collection must be populated during the batched load");
            }
            assertTrue(counting.getPreparedStatements() <= 4,
                    "expected 1 query for companies plus 2 batched collection queries, got "
                            + counting.getPreparedStatements());
        } finally {
            em.close();
        }
    }
}
