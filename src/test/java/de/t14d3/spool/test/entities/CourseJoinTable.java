package de.t14d3.spool.test.entities;

import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Entity;
import de.t14d3.spool.annotations.Id;
import de.t14d3.spool.annotations.ManyToMany;
import de.t14d3.spool.annotations.Table;

import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "courses_jt")
public class CourseJoinTable {
    @Id(autoIncrement = true)
    @Column(name = "id")
    private Long id;

    @Column(name = "title")
    private String title;

    @ManyToMany(mappedBy = "courses")
    private List<StudentJoinTable> students = new ArrayList<>();

    public CourseJoinTable() {}

    public CourseJoinTable(String title) {
        this.title = title;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<StudentJoinTable> getStudents() {
        return students;
    }

    public void setStudents(List<StudentJoinTable> students) {
        this.students = students;
    }
}

