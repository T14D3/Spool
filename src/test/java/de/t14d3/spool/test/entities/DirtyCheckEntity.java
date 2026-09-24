package de.t14d3.spool.test.entities;

import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Entity;
import de.t14d3.spool.annotations.Id;
import de.t14d3.spool.annotations.JoinColumn;
import de.t14d3.spool.annotations.ManyToOne;
import de.t14d3.spool.annotations.PreUpdate;
import de.t14d3.spool.annotations.Table;

/**
 * Entity with a {@code ManyToOne} and a {@code byte[]} column, used to verify
 * that dirty comparison handles normalized relationship values and arrays.
 */
@Entity
@Table(name = "dirty_check_entities")
public class DirtyCheckEntity {
    public static int preUpdateCount = 0;

    @Id(autoIncrement = true)
    @Column(name = "id")
    private Long id;

    @Column(name = "name")
    private String name;

    @Column(name = "payload")
    private byte[] payload;

    @ManyToOne
    @JoinColumn(name = "owner_id")
    private User owner;

    public DirtyCheckEntity() {}

    public DirtyCheckEntity(String name, byte[] payload, User owner) {
        this.name = name;
        this.payload = payload;
        this.owner = owner;
    }

    @PreUpdate
    void onPreUpdate() {
        preUpdateCount++;
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public byte[] getPayload() { return payload; }
    public void setPayload(byte[] payload) { this.payload = payload; }
    public User getOwner() { return owner; }
    public void setOwner(User owner) { this.owner = owner; }
}
