package de.t14d3.spool.mapping;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.UUID;

/**
 * Centralized type mapping utilities for converting between Java types and SQL types,
 * and for converting database values to Java types.
 */
public class TypeMapper {

    /**
     * Convert a Java class to its corresponding SQL type string.
     */
    public static String javaTypeToSqlType(Class<?> javaType) {
        if (javaType == String.class) {
            return "VARCHAR";
        } else if (javaType == UUID.class) {
            return "VARCHAR";
        } else if (javaType == Long.class || javaType == long.class) {
            return "BIGINT";
        } else if (javaType == Integer.class || javaType == int.class) {
            return "INTEGER";
        } else if (javaType == Short.class || javaType == short.class) {
            return "SMALLINT";
        } else if (javaType == Byte.class || javaType == byte.class) {
            return "TINYINT";
        } else if (javaType == Double.class || javaType == double.class) {
            return "DOUBLE";
        } else if (javaType == Float.class || javaType == float.class) {
            return "FLOAT";
        } else if (javaType == Boolean.class || javaType == boolean.class) {
            return "BOOLEAN";
        } else if (javaType == java.util.Date.class || javaType == java.sql.Date.class) {
            return "DATE";
        } else if (javaType == java.sql.Timestamp.class) {
            return "TIMESTAMP";
        } else if (javaType == LocalDate.class) {
            return "DATE";
        } else if (javaType == LocalDateTime.class) {
            return "TIMESTAMP";
        } else if (javaType == byte[].class) {
            return "BLOB";
        } else if (javaType == BigDecimal.class) {
            return "DECIMAL";
        } else {
            // For entity types (relationships), use BIGINT for foreign keys
            return "BIGINT";
        }
    }

    /**
     * Convert a database value to the target Java type.
     * Handles type coercion where appropriate (e.g., Number -> int, long, float, double).
     */
    public static Object convertToJavaType(Object value, Class<?> targetType) {
        if (value == null) return null;

        if (targetType == Long.class || targetType == long.class) {
            if (value instanceof Number) return ((Number) value).longValue();
        } else if (targetType == Integer.class || targetType == int.class) {
            if (value instanceof Number) return ((Number) value).intValue();
        } else if (targetType == Double.class || targetType == double.class) {
            if (value instanceof Number) return ((Number) value).doubleValue();
        } else if (targetType == Float.class || targetType == float.class) {
            if (value instanceof Number) return ((Number) value).floatValue();
        } else if (targetType == Short.class || targetType == short.class) {
            if (value instanceof Number) return ((Number) value).shortValue();
        } else if (targetType == Byte.class || targetType == byte.class) {
            if (value instanceof Number) return ((Number) value).byteValue();
        } else if (targetType == UUID.class) {
            if (value instanceof String) {
                return UUID.fromString((String) value);
            } else if (value instanceof UUID) {
                return value;
            }
        } else if (targetType == String.class) {
            return value.toString();
        } else if (targetType == Boolean.class || targetType == boolean.class) {
            if (value instanceof Boolean) return value;
            if (value instanceof Number) return ((Number) value).intValue() != 0;
            return Boolean.parseBoolean(value.toString());
        } else if (targetType == java.util.Date.class) {
            if (value instanceof Timestamp) {
                return new java.util.Date(((Timestamp) value).getTime());
            } else if (value instanceof java.sql.Date) {
                return new java.util.Date(((java.sql.Date) value).getTime());
            } else if (value instanceof LocalDateTime) {
                return java.util.Date.from(((LocalDateTime) value).atZone(ZoneId.systemDefault()).toInstant());
            }
        } else if (targetType == LocalDate.class) {
            if (value instanceof java.sql.Date) {
                return ((java.sql.Date) value).toLocalDate();
            } else if (value instanceof Timestamp) {
                return ((Timestamp) value).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
            } else if (value instanceof LocalDateTime) {
                return ((LocalDateTime) value).toLocalDate();
            }
        } else if (targetType == LocalDateTime.class) {
            if (value instanceof Timestamp) {
                return ((Timestamp) value).toLocalDateTime();
            } else if (value instanceof java.sql.Date) {
                return ((java.sql.Date) value).toLocalDate().atStartOfDay();
            }
        } else if (targetType == BigDecimal.class) {
            if (value instanceof Number) {
                return BigDecimal.valueOf(((Number) value).doubleValue());
            } else if (value instanceof String) {
                return new BigDecimal((String) value);
            }
        } else if (targetType == byte[].class) {
            if (value instanceof Blob) {
                try {
                    return ((Blob) value).getBytes(1, (int) ((Blob) value).length());
                } catch (Exception e) {
                    throw new RuntimeException("Failed to read blob", e);
                }
            }
        }

        // For types we don't explicitly handle (e.g., enums, custom types), return as-is
        // and let reflection handle it, which may succeed or fail appropriately.
        return value;
    }
}
