package de.t14d3.spool.test.entities;

import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Entity;
import de.t14d3.spool.annotations.Id;
import de.t14d3.spool.annotations.PostPersist;
import de.t14d3.spool.annotations.PostRemove;
import de.t14d3.spool.annotations.PostUpdate;
import de.t14d3.spool.annotations.PreLoad;
import de.t14d3.spool.annotations.PrePersist;
import de.t14d3.spool.annotations.PreRemove;
import de.t14d3.spool.annotations.PreUpdate;
import de.t14d3.spool.annotations.Table;

import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "lifecycle_entities")
public class LifecycleEntity {
    public static final List<String> events = new ArrayList<>();
    public static boolean failOnPrePersist = false;

    @Id(autoIncrement = true)
    @Column(name = "id")
    private Long id;

    @Column(name = "name")
    private String name;

    public LifecycleEntity() {}

    public LifecycleEntity(String name) {
        this.name = name;
    }

    @PrePersist
    void onPrePersist() {
        if (failOnPrePersist) {
            throw new IllegalStateException("pre-persist failure");
        }
        events.add("prePersist:" + name);
    }

    @PostPersist
    void onPostPersist() {
        events.add("postPersist:" + (id != null));
    }

    @PreUpdate
    void onPreUpdate() {
        events.add("preUpdate:" + name);
    }

    @PostUpdate
    void onPostUpdate() {
        events.add("postUpdate:" + name);
    }

    @PreRemove
    void onPreRemove() {
        events.add("preRemove:" + name);
    }

    @PostRemove
    void onPostRemove() {
        events.add("postRemove:" + name);
    }

    @PreLoad
    void onPreLoad() {
        events.add("preLoad:" + name);
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
}
