package de.t14d3.spool.test.entities;

import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Entity;
import de.t14d3.spool.annotations.Id;
import de.t14d3.spool.annotations.JoinTable;
import de.t14d3.spool.annotations.ManyToMany;
import de.t14d3.spool.annotations.Table;

import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "students_jt")
public class StudentJoinTable {
    @Id(autoIncrement = true)
    @Column(name = "id")
    private Long id;

    @Column(name = "name")
    private String name;

    @ManyToMany
    @JoinTable(name = "enrollments", joinColumn = "student_ref", inverseJoinColumn = "course_ref")
    private List<CourseJoinTable> courses = new ArrayList<>();

    public StudentJoinTable() {}

    public StudentJoinTable(String name) {
        this.name = name;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<CourseJoinTable> getCourses() {
        return courses;
    }

    public void setCourses(List<CourseJoinTable> courses) {
        this.courses = courses;
    }
}

