package de.t14d3.spool.test.entities;

import de.t14d3.spool.annotations.CascadeType;
import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Entity;
import de.t14d3.spool.annotations.Id;
import de.t14d3.spool.annotations.OneToMany;
import de.t14d3.spool.annotations.OrderBy;
import de.t14d3.spool.annotations.Table;

import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "ordered_parents")
public class OrderedParent {
    @Id(autoIncrement = true)
    @Column(name = "id")
    private Long id;

    @Column(name = "name")
    private String name;

    @OneToMany(targetEntity = OrderedChild.class, mappedBy = "parent", cascade = CascadeType.ALL)
    @OrderBy("name DESC")
    private List<OrderedChild> children = new ArrayList<>();

    public OrderedParent() {}

    public OrderedParent(String name) {
        this.name = name;
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public List<OrderedChild> getChildren() {
        return children;
    }
}
