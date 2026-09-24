package de.t14d3.spool.test.entities;

import de.t14d3.spool.annotations.CascadeType;
import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Entity;
import de.t14d3.spool.annotations.Id;
import de.t14d3.spool.annotations.OneToOne;
import de.t14d3.spool.annotations.Table;

@Entity
@Table(name = "persons")
public class Person {
    @Id(autoIncrement = true)
    @Column(name = "id")
    private Long id;

    @Column(name = "name")
    private String name;

    /** Inverse side: the FK column lives on {@code passports}. */
    @OneToOne(mappedBy = "person", cascade = CascadeType.ALL)
    private Passport passport;

    public Person() {}

    public Person(String name) {
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

    public Passport getPassport() {
        return passport;
    }

    public void setPassport(Passport passport) {
        this.passport = passport;
    }
}
