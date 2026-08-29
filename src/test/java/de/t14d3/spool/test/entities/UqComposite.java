package de.t14d3.spool.test.entities;

import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Entity;
import de.t14d3.spool.annotations.Id;
import de.t14d3.spool.annotations.Table;
import de.t14d3.spool.annotations.UniqueConstraint;

@Entity
@Table(name = "uq_composite",
        uniqueConstraints = {
            @UniqueConstraint(name = "uq_composite_owner_repo", columns = {"owner_id", "repo_id"})
        })
public class UqComposite {
    @Id(autoIncrement = true)
    @Column(name = "id")
    private Long id;

    @Column(name = "owner_id")
    private String ownerId;

    @Column(name = "repo_id")
    private String repoId;

    public UqComposite() {}

    public UqComposite(String ownerId, String repoId) {
        this.ownerId = ownerId;
        this.repoId = repoId;
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getOwnerId() { return ownerId; }
    public void setOwnerId(String ownerId) { this.ownerId = ownerId; }

    public String getRepoId() { return repoId; }
    public void setRepoId(String repoId) { this.repoId = repoId; }
}
