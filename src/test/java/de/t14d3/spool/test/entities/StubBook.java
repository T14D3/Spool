package de.t14d3.spool.test.entities;

import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Entity;
import de.t14d3.spool.annotations.FetchType;
import de.t14d3.spool.annotations.Id;
import de.t14d3.spool.annotations.JoinColumn;
import de.t14d3.spool.annotations.ManyToOne;
import de.t14d3.spool.annotations.Table;

/**
 * Carries a LAZY {@code @ManyToOne}: the author field stays an id-only stub
 * until an explicit fetch resolves it.
 */
@Entity
@Table(name = "stub_books")
public class StubBook {
    @Id(autoIncrement = true)
    @Column(name = "id")
    private Long id;

    @Column(name = "title")
    private String title;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "author_id")
    private StubAuthor author;

    public StubBook() {}

    public StubBook(String title) {
        this.title = title;
    }

    public Long getId() {
        return id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public StubAuthor getAuthor() {
        return author;
    }

    public void setAuthor(StubAuthor author) {
        this.author = author;
    }
}
