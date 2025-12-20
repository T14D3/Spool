package de.t14d3.spool.test.entities;

import de.t14d3.spool.annotations.CascadeType;
import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Entity;
import de.t14d3.spool.annotations.Id;
import de.t14d3.spool.annotations.JoinColumn;
import de.t14d3.spool.annotations.ManyToOne;
import de.t14d3.spool.annotations.Table;

@Entity
@Table(name = "homes_cjc")
public class HomeCustomJoinColumn {
    @Id(autoIncrement = true)
    @Column(name = "id")
    private Long id;

    @Column(name = "name")
    private String name;

    @ManyToOne(cascade = CascadeType.ALL)
    @JoinColumn(name = "player_uuid")
    private PlayerCustomJoinColumn player;

    public HomeCustomJoinColumn() {}

    public HomeCustomJoinColumn(String name) {
        this.name = name;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public PlayerCustomJoinColumn getPlayer() {
        return player;
    }

    public void setPlayer(PlayerCustomJoinColumn player) {
        this.player = player;
    }
}

