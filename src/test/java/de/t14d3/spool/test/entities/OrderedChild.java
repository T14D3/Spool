package de.t14d3.spool.test.entities;

import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Entity;
import de.t14d3.spool.annotations.Id;
import de.t14d3.spool.annotations.JoinColumn;
import de.t14d3.spool.annotations.ManyToOne;
import de.t14d3.spool.annotations.Table;

@Entity
@Table(name = "ordered_children")
public class OrderedChild {
    @Id(autoIncrement = true)
    @Column(name = "id")
    private Long id;

    @Column(name = "name")
    private String name;

    @ManyToOne
    @JoinColumn(name = "parent_id")
    private OrderedParent parent;

    public OrderedChild() {}

    public OrderedChild(String name) {
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

    public OrderedParent getParent() {
        return parent;
    }

    public void setParent(OrderedParent parent) {
        this.parent = parent;
    }
}
