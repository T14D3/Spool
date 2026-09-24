package de.t14d3.spool.test.entities;

import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Id;
import de.t14d3.spool.annotations.MappedSuperclass;
import de.t14d3.spool.annotations.Version;

import java.time.LocalDateTime;

/**
 * Shared id + audit columns for {@link MappedSuperclassUser} and
 * {@link MappedSuperclassOrder}. No table of its own.
 */
@MappedSuperclass
public abstract class BaseEntity {
    @Id(autoIncrement = true)
    @Column(name = "id")
    private Long id;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Version
    @Column(name = "version", nullable = false)
    private Long version;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public Long getVersion() {
        return version;
    }
}
