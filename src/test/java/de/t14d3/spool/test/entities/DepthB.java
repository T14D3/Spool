package de.t14d3.spool.test.entities;

import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Entity;
import de.t14d3.spool.annotations.FetchType;
import de.t14d3.spool.annotations.Id;
import de.t14d3.spool.annotations.JoinColumn;
import de.t14d3.spool.annotations.ManyToOne;
import de.t14d3.spool.annotations.Table;

@Entity
@Table(name = "depth_bs")
public class DepthB {
    @Id(autoIncrement = true)
    @Column(name = "id")
    private Long id;

    @Column(name = "name")
    private String name;

    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "c_id")
    private DepthC c;

    public DepthB() {}

    public DepthB(String name) {
        this.name = name;
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public DepthC getC() {
        return c;
    }

    public void setC(DepthC c) {
        this.c = c;
    }
}
