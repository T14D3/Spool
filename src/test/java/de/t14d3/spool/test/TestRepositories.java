package de.t14d3.spool.test;

import de.t14d3.spool.core.EntityManager;
import de.t14d3.spool.repository.EntityRepository;
import de.t14d3.spool.test.entities.Author;
import de.t14d3.spool.test.entities.Book;
import de.t14d3.spool.test.entities.User;

import java.util.List;

class UserRepository extends EntityRepository<User> {
    public UserRepository(EntityManager em) {
        super(em, User.class);
    }
}

class AuthorRepository extends EntityRepository<Author> {
    public AuthorRepository(EntityManager em) {
        super(em, Author.class);
    }

    public List<Author> findByName(String name) {
        return findAll().stream()
                .filter(a -> a.getName().equals(name))
                .toList();
    }
}

class BookRepository extends EntityRepository<Book> {
    public BookRepository(EntityManager em) {
        super(em, Book.class);
    }

    public List<Book> findByTitle(String title) {
        return findAll().stream()
                .filter(b -> title.equals(b.getTitle()))
                .toList();
    }
}