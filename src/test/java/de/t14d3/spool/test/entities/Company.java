package de.t14d3.spool.test.entities;

import de.t14d3.spool.annotations.CascadeType;
import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Entity;
import de.t14d3.spool.annotations.FetchType;
import de.t14d3.spool.annotations.Id;
import de.t14d3.spool.annotations.JoinColumn;
import de.t14d3.spool.annotations.OneToMany;
import de.t14d3.spool.annotations.Table;

import java.util.ArrayList;
import java.util.List;

/**
 * EAGER unidirectional {@code @OneToMany}, used to prove the eager path also
 * manages the FK column on the target table.
 */
@Entity
@Table(name = "companies")
public class Company {
    @Id(autoIncrement = true)
    @Column(name = "id")
    private Long id;

    @Column(name = "name")
    private String name;

    @OneToMany(targetEntity = Employee.class, mappedBy = "", fetch = FetchType.EAGER, cascade = CascadeType.PERSIST)
    @JoinColumn(name = "company_id")
    private List<Employee> employees = new ArrayList<>();

    public Company() {}

    public Company(String name) {
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

    public List<Employee> getEmployees() {
        return employees;
    }
}
