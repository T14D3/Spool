package de.t14d3.spool.test.entities;

import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Entity;
import de.t14d3.spool.annotations.Id;
import de.t14d3.spool.annotations.Table;

@Entity
@Table(name = "order_lines")
public class OrderLine {
    @Id
    @Column(name = "order_id")
    private Long orderId;

    @Id
    @Column(name = "line_no")
    private Integer lineNo;

    @Column(name = "sku")
    private String sku;

    @Column(name = "quantity")
    private Integer quantity;

    public OrderLine() {}

    public OrderLine(Long orderId, Integer lineNo, String sku, Integer quantity) {
        this.orderId = orderId;
        this.lineNo = lineNo;
        this.sku = sku;
        this.quantity = quantity;
    }

    public Long getOrderId() { return orderId; }
    public void setOrderId(Long orderId) { this.orderId = orderId; }
    public Integer getLineNo() { return lineNo; }
    public void setLineNo(Integer lineNo) { this.lineNo = lineNo; }
    public String getSku() { return sku; }
    public void setSku(String sku) { this.sku = sku; }
    public Integer getQuantity() { return quantity; }
    public void setQuantity(Integer quantity) { this.quantity = quantity; }
}
