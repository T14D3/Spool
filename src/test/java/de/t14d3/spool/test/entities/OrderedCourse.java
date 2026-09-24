package de.t14d3.spool.test.entities;

import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Entity;
import de.t14d3.spool.annotations.Id;
import de.t14d3.spool.annotations.ManyToMany;
import de.t14d3.spool.annotations.Table;

import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "ordered_courses")
public class OrderedCourse {
    @Id(autoIncrement = true)
    @Column(name = "id")
    private Long id;

    @Column(name = "title")
    private String title;

    @ManyToMany(mappedBy = "courses")
    private List<OrderedStudent> students = new ArrayList<>();

    public OrderedCourse() {}

    public OrderedCourse(String title) {
        this.title = title;
    }

    public Long getId() {
        return id;
    }

    public String getTitle() {
        return title;
    }

    public List<OrderedStudent> getStudents() {
        return students;
    }
}
