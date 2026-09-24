package de.t14d3.spool.test.entities;

import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Entity;
import de.t14d3.spool.annotations.Table;

@Entity
@Table(name = "mapped_superclass_users")
public class MappedSuperclassUser extends BaseEntity {
    @Column(name = "name")
    private String name;

    public MappedSuperclassUser() {}

    public MappedSuperclassUser(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
