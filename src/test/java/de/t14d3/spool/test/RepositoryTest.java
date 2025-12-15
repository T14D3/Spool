package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.repository.EntityRepository;
import de.t14d3.spool.test.entities.User;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for repository pattern functionality.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RepositoryTest {
    private EntityManager em;
    private EntityRepository<User> userRepo;

    @BeforeEach
    void setup() {
        em = EntityManager.create("jdbc:h2:mem:repo_test;DB_CLOSE_DELAY=-1");
        userRepo = new EntityRepository<>(em, User.class);

        // Drop and recreate tables to ensure clean state
        try {
            em.getExecutor().execute("DROP TABLE IF EXISTS users", List.of());
        } catch (Exception e) {
            // Ignore - table might not exist
        }

        em.getExecutor().execute(
                "CREATE TABLE users (" +
                        "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
                        "username VARCHAR(50), " +
                        "email VARCHAR(50)" +
                        ")", List.of()
        );
    }

    @AfterEach
    void teardown() {
        em.close();
    }

    @Test
    void testSaveAndFindById() {
        // Create and save a user
        User user = new User("john_doe", "john@example.com");
        assertNull(user.getId());
        
        userRepo.save(user);
        em.flush();
        
        assertNotNull(user.getId());
        
        // Find by ID
        User foundUser = userRepo.findById(user.getId());
        assertNotNull(foundUser);
        assertEquals("john_doe", foundUser.getUsername());
        assertEquals("john@example.com", foundUser.getEmail());
    }

    @Test
    void testUpdateEntity() {
        // Create and save a user
        User user = new User("jane_doe", "jane@example.com");
        userRepo.save(user);
        em.flush();
        
        // Update the user
        user.setEmail("jane.updated@example.com");
        userRepo.save(user);
        em.flush();
        
        // Verify update
        User updatedUser = userRepo.findById(user.getId());
        assertNotNull(updatedUser);
        assertEquals("jane.updated@example.com", updatedUser.getEmail());
    }

    @Test
    void testDelete() {
        // Create and save a user
        User user = new User("delete_me", "delete@example.com");
        userRepo.save(user);
        em.flush();
        
        Long userId = user.getId();
        
        // Delete the user
        userRepo.delete(user);
        em.flush();
        
        // Verify deletion
        User deletedUser = userRepo.findById(userId);
        assertNull(deletedUser);
    }

    @Test
    void testUpdateViaRepository() {
        // Create and save multiple users
        User user1 = new User("user1", "user1@example.com");
        User user2 = new User("user2", "user2@example.com");
        User user3 = new User("user3", "user3@example.com");
        
        userRepo.save(user1);
        userRepo.save(user2);
        userRepo.save(user3);
        em.flush();
        
        // Test count
        assertEquals(3, userRepo.count());
        
        // Test findAll
        List<User> allUsers = userRepo.findAll();
        assertEquals(3, allUsers.size());
        
        // Test existsById
        assertTrue(userRepo.existsById(user1.getId()));
        assertTrue(userRepo.existsById(user2.getId()));
        assertTrue(userRepo.existsById(user3.getId()));
        assertFalse(userRepo.existsById(999L));
        
        // Test deleteById
        userRepo.deleteById(user3.getId());
        em.flush();
        
        assertEquals(2, userRepo.count());
        assertFalse(userRepo.existsById(user3.getId()));
    }

    @Test
    void testFindBy() {
        // Create and save a user
        User user = new User("john_doe", "john@example.com");
        assertNull(user.getId());

        userRepo.save(user);
        em.flush();

        assertNotNull(user.getId());

        // Find by ID
        User foundUser = userRepo.findById(user.getId());
        assertNotNull(foundUser);
        assertEquals("john_doe", foundUser.getUsername());
        assertEquals("john@example.com", foundUser.getEmail());

        // Find by username
        List<User> foundUsers = userRepo.findBy("username", "john_doe");
        assertEquals(1, foundUsers.size());
        assertEquals("john_doe", foundUsers.get(0).getUsername());

        // Find by email
        foundUsers = userRepo.findBy("email", "john@example.com");
        assertEquals(1, foundUsers.size());
        assertEquals("john_doe", foundUsers.get(0).getUsername());

        // Find one by
        foundUser = userRepo.findOneBy("username", "john_doe");
        assertNotNull(foundUser);
        assertEquals("john_doe", foundUser.getUsername());

        foundUser = userRepo.findOneBy("email", "john@example.com");
        assertNotNull(foundUser);
        assertEquals("john_doe", foundUser.getUsername());
    }
}
