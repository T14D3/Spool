package de.t14d3.spool.test.entities;

import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Entity;
import de.t14d3.spool.annotations.Id;
import de.t14d3.spool.annotations.Table;

/**
 * Owner of a {@link PrimitiveIdTicket}, with a primitive {@code int} id.
 * <p>
 * A primitive id cannot be {@code null}, so {@code 0} is the only way to say
 * "not inserted yet". This mirrors the ticket/category pair of the tickets
 * demo application and pins the transient-detection behaviour for that shape.
 */
@Entity
@Table(name = "primitive_id_categories")
public class PrimitiveIdCategory {
    @Id(autoIncrement = true)
    @Column(name = "id")
    private int id;

    @Column(name = "name")
    private String name;

    public PrimitiveIdCategory() {}

    public PrimitiveIdCategory(String name) {
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
