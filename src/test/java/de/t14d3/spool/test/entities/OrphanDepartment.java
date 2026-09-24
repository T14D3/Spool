package de.t14d3.spool.test.entities;

import de.t14d3.spool.annotations.CascadeType;
import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Entity;
import de.t14d3.spool.annotations.Id;
import de.t14d3.spool.annotations.JoinColumn;
import de.t14d3.spool.annotations.OneToMany;
import de.t14d3.spool.annotations.Table;

import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "orphan_departments")
public class OrphanDepartment {
    @Id(autoIncrement = true)
    @Column(name = "id")
    private Long id;

    @Column(name = "name")
    private String name;

    @OneToMany(targetEntity = OrphanEmployee.class, mappedBy = "", cascade = CascadeType.PERSIST, orphanRemoval = true)
    @JoinColumn(name = "department_id")
    private List<OrphanEmployee> employees = new ArrayList<>();

    public OrphanDepartment() {}

    public OrphanDepartment(String name) {
        this.name = name;
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public List<OrphanEmployee> getEmployees() {
        return employees;
    }
}
