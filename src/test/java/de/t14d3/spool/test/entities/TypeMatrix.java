package de.t14d3.spool.test.entities;

import de.t14d3.spool.annotations.Column;
import de.t14d3.spool.annotations.Entity;
import de.t14d3.spool.annotations.Id;
import de.t14d3.spool.annotations.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * One column per supported Java type, used for persist/load round-trip tests.
 */
@Entity
@Table(name = "type_matrix")
public class TypeMatrix {
    public enum Status {
        ACTIVE,
        ARCHIVED
    }

    @Id(autoIncrement = true)
    @Column(name = "id")
    private Long id;

    @Column(name = "text_value")
    private String textValue;

    @Column(name = "int_value")
    private Integer intValue;

    @Column(name = "long_value")
    private Long longValue;

    @Column(name = "double_value")
    private Double doubleValue;

    @Column(name = "flag")
    private Boolean flag;

    @Column(name = "amount")
    private BigDecimal amount;

    @Column(name = "local_date")
    private LocalDate localDate;

    @Column(name = "local_dt")
    private LocalDateTime localDateTime;

    @Column(name = "instant_value")
    private Instant instantValue;

    @Column(name = "offset_dt")
    private OffsetDateTime offsetDateTime;

    @Column(name = "uuid_value")
    private UUID uuidValue;

    @Column(name = "blob_value")
    private byte[] blobValue;

    @Column(name = "status")
    private Status status;

    public TypeMatrix() {}

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getTextValue() { return textValue; }
    public void setTextValue(String textValue) { this.textValue = textValue; }
    public Integer getIntValue() { return intValue; }
    public void setIntValue(Integer intValue) { this.intValue = intValue; }
    public Long getLongValue() { return longValue; }
    public void setLongValue(Long longValue) { this.longValue = longValue; }
    public Double getDoubleValue() { return doubleValue; }
    public void setDoubleValue(Double doubleValue) { this.doubleValue = doubleValue; }
    public Boolean getFlag() { return flag; }
    public void setFlag(Boolean flag) { this.flag = flag; }
    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
    public LocalDate getLocalDate() { return localDate; }
    public void setLocalDate(LocalDate localDate) { this.localDate = localDate; }
    public LocalDateTime getLocalDateTime() { return localDateTime; }
    public void setLocalDateTime(LocalDateTime localDateTime) { this.localDateTime = localDateTime; }
    public Instant getInstantValue() { return instantValue; }
    public void setInstantValue(Instant instantValue) { this.instantValue = instantValue; }
    public OffsetDateTime getOffsetDateTime() { return offsetDateTime; }
    public void setOffsetDateTime(OffsetDateTime offsetDateTime) { this.offsetDateTime = offsetDateTime; }
    public UUID getUuidValue() { return uuidValue; }
    public void setUuidValue(UUID uuidValue) { this.uuidValue = uuidValue; }
    public byte[] getBlobValue() { return blobValue; }
    public void setBlobValue(byte[] blobValue) { this.blobValue = blobValue; }
    public Status getStatus() { return status; }
    public void setStatus(Status status) { this.status = status; }
}
