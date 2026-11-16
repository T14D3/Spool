package de.t14d3.spool.test.entities;

import de.t14d3.spool.annotations.CascadeType;
import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Entity;
import de.t14d3.spool.annotations.Id;
import de.t14d3.spool.annotations.ManyToOne;
import de.t14d3.spool.annotations.Table;

/**
 * Child of a {@link PrimitiveIdCategory} with a primitive {@code int} id and
 * a cascading {@code @ManyToOne}.
 */
@Entity
@Table(name = "primitive_id_tickets")
public class PrimitiveIdTicket {
    @Id(autoIncrement = true)
    @Column(name = "id")
    private int id;

    @ManyToOne(cascade = CascadeType.PERSIST)
    private PrimitiveIdCategory category;

    @Column(name = "title")
    private String title;

    public PrimitiveIdTicket() {}

    public PrimitiveIdTicket(String title) {
        this.title = title;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public PrimitiveIdCategory getCategory() {
        return category;
    }

    public void setCategory(PrimitiveIdCategory category) {
        this.category = category;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }
}
