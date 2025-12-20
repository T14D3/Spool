package de.t14d3.spool.test.entities;

import de.t14d3.spool.annotations.CascadeType;
import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Entity;
import de.t14d3.spool.annotations.Id;
import de.t14d3.spool.annotations.OneToMany;
import de.t14d3.spool.annotations.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Entity
@Table(name = "players_cjc")
public class PlayerCustomJoinColumn {
    @Id
    @Column(name = "uuid")
    private UUID uuid;

    @Column(name = "name")
    private String name;

    @OneToMany(targetEntity = HomeCustomJoinColumn.class, mappedBy = "player", cascade = CascadeType.ALL)
    private List<HomeCustomJoinColumn> homes = new ArrayList<>();

    public PlayerCustomJoinColumn() {}

    public PlayerCustomJoinColumn(UUID uuid, String name) {
        this.uuid = uuid;
        this.name = name;
    }

    public UUID getUuid() {
        return uuid;
    }

    public void setUuid(UUID uuid) {
        this.uuid = uuid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<HomeCustomJoinColumn> getHomes() {
        return homes;
    }

    public void addHome(HomeCustomJoinColumn home) {
        homes.add(home);
        home.setPlayer(this);
    }
}

