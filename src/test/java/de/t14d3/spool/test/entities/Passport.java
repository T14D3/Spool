package de.t14d3.spool.test.entities;

import de.t14d3.spool.annotations.CascadeType;
import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Entity;
import de.t14d3.spool.annotations.Id;
import de.t14d3.spool.annotations.JoinColumn;
import de.t14d3.spool.annotations.OneToOne;
import de.t14d3.spool.annotations.Table;

@Entity
@Table(name = "passports")
public class Passport {
    @Id(autoIncrement = true)
    @Column(name = "id")
    private Long id;

    @Column(name = "number")
    private String number;

    /** Owning side: this table carries the FK column. */
    @OneToOne(cascade = CascadeType.ALL)
    @JoinColumn(name = "person_id")
    private Person person;

    public Passport() {}

    public Passport(String number) {
        this.number = number;
    }

    public Long getId() {
        return id;
    }

    public String getNumber() {
        return number;
    }

    public void setNumber(String number) {
        this.number = number;
    }

    public Person getPerson() {
        return person;
    }

    public void setPerson(Person person) {
        this.person = person;
    }
}
