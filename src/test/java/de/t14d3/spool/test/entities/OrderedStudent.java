package de.t14d3.spool.test.entities;

import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Entity;
import de.t14d3.spool.annotations.Id;
import de.t14d3.spool.annotations.ManyToMany;
import de.t14d3.spool.annotations.OrderBy;
import de.t14d3.spool.annotations.Table;

import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "ordered_students")
public class OrderedStudent {
    @Id(autoIncrement = true)
    @Column(name = "id")
    private Long id;

    @Column(name = "name")
    private String name;

    @ManyToMany
    @OrderBy("title DESC")
    private List<OrderedCourse> courses = new ArrayList<>();

    public OrderedStudent() {}

    public OrderedStudent(String name) {
        this.name = name;
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public List<OrderedCourse> getCourses() {
        return courses;
    }
}
