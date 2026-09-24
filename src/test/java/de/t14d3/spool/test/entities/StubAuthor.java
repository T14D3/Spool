package de.t14d3.spool.test.entities;

import de.t14d3.spool.annotations.CascadeType;
import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Entity;
import de.t14d3.spool.annotations.Id;
import de.t14d3.spool.annotations.OneToMany;
import de.t14d3.spool.annotations.Table;

import java.util.ArrayList;
import java.util.List;

/**
 * Owner of a LAZY collection; used to exercise the explicit fetch API.
 */
@Entity
@Table(name = "stub_authors")
public class StubAuthor {
    @Id(autoIncrement = true)
    @Column(name = "id")
    private Long id;

    @Column(name = "name")
    private String name;

    @OneToMany(targetEntity = StubBook.class, mappedBy = "author", cascade = CascadeType.PERSIST)
    private List<StubBook> books = new ArrayList<>();

    public StubAuthor() {}

    public StubAuthor(String name) {
        this.name = name;
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<StubBook> getBooks() {
        return books;
    }
}
