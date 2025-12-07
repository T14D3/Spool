package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.repository.EntityRepository;
import de.t14d3.spool.test.entities.User;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Demo test showing ORM functionality.
 */


public class OrmDemoTest {

    @Test
    public void testOrmDemo() {
        System.out.println("=== Spool ORM Demo ===\n");

        // Create EntityManager with H2 in-memory database
        EntityManager em = EntityManager.create("jdbc:h2:mem:demo;DB_CLOSE_DELAY=-1");

        try {
            // Create the table (in a real app, you'd use migrations)
            em.getExecutor().execute(
                    "CREATE TABLE users (id BIGINT AUTO_INCREMENT PRIMARY KEY, username VARCHAR(50), email VARCHAR(50))", List.of()
            );

            System.out.println("1. Direct EntityManager Usage:");
            System.out.println("-----------------------------");

            // Create and persist entities
            User alice = new User(null, "alice", "alice@example.com");
            User bob = new User(null, "bob", "bob@example.com");
            User carol = new User(null, "carol", "carol@example.com");

            em.persist(alice);
            em.persist(bob);
            em.persist(carol);
            em.flush(); // Write to database

            System.out.println("Created users:");
            System.out.println("  - " + alice.getUsername() + " (" + alice.getEmail() + ") ID: " + alice.getId());
            System.out.println("  - " + bob.getUsername() + " (" + bob.getEmail() + ") ID: " + bob.getId());
            System.out.println("  - " + carol.getUsername() + " (" + carol.getEmail() + ") ID: " + carol.getId());

            // Find entity by ID
            User found = em.find(User.class, alice.getId());
            System.out.println("\nFound: " + found.getUsername() + " - " + found.getEmail());

            // Update entity
            found.setEmail("alice.updated@example.com");
            em.persist(found);
            em.flush();
            System.out.println("Updated email to: " + found.getEmail());

            // Remove entity
            em.remove(bob);
            em.flush();
            System.out.println("Removed: " + bob.getUsername());

            System.out.println("\n2. Repository Pattern Usage:");
            System.out.println("----------------------------");

            // Create repository
            UserRepository repo = new UserRepository(em);

            // Find all
            List<User> allUsers = repo.findAll();
            System.out.println("All users in database: " + allUsers.size());
            for (User u : allUsers) {
                System.out.println("  - " + u.getUsername() + " (" + u.getEmail() + ")");
            }

            // Add new user via repository
            User dave = new User(null, "dave", "dave@example.com");
            repo.save(dave);
            em.flush();
            System.out.println("\nAdded via repository: " + dave.getUsername() + " (ID: " + dave.getId() + ")");

            // Check existence
            boolean exists = repo.existsById(dave.getId());
            System.out.println("User exists: " + exists);

            // Count
            long count = repo.count();
            System.out.println("Total users: " + count);

            // Delete by ID
            repo.deleteById(carol.getId());
            em.flush();
            System.out.println("Deleted carol by ID");

            // Final count
            count = repo.count();
            System.out.println("Final user count: " + count);

            System.out.println("\n=== Demo Complete ===");

        } finally {
            em.close();
        }
    }
}
