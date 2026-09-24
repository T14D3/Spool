package de.t14d3.spool.test.entities;

import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Entity;
import de.t14d3.spool.annotations.Table;

import java.math.BigDecimal;

@Entity
@Table(name = "mapped_superclass_orders")
public class MappedSuperclassOrder extends BaseEntity {
    @Column(name = "total")
    private BigDecimal total;

    public MappedSuperclassOrder() {}

    public MappedSuperclassOrder(BigDecimal total) {
        this.total = total;
    }

    public BigDecimal getTotal() {
        return total;
    }

    public void setTotal(BigDecimal total) {
        this.total = total;
    }
}
