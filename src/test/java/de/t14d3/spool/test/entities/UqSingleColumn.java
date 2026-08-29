package de.t14d3.spool.test.entities;

import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Entity;
import de.t14d3.spool.annotations.Id;
import de.t14d3.spool.annotations.Table;
import de.t14d3.spool.annotations.UniqueConstraint;

@Entity
@Table(name = "uq_single_column",
        uniqueConstraints = {
            @UniqueConstraint(name = "uq_single_email", columns = {"email"})
        })
public class UqSingleColumn {
    @Id(autoIncrement = true)
    @Column(name = "id")
    private Long id;

    @Column(name = "email")
    private String email;

    public UqSingleColumn() {}

    public UqSingleColumn(String email) {
        this.email = email;
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }
}
