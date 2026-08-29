package de.t14d3.spool.test.entities;

import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Entity;
import de.t14d3.spool.annotations.Id;
import de.t14d3.spool.annotations.Table;

@Entity
@Table(name = "uq_column_flag")
public class UqColumnFlag {
    @Id(autoIncrement = true)
    @Column(name = "id")
    private Long id;

    @Column(name = "email", unique = true)
    private String email;

    public UqColumnFlag() {}

    public UqColumnFlag(String email) {
        this.email = email;
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }
}
