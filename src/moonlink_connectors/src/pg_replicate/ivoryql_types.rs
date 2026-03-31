/// IvorySQL Oracle-compatible type support.
///
/// IvorySQL extends PostgreSQL with Oracle-mode types registered under a separate schema
/// (typically `sys`). These types have custom OIDs not recognized by
/// `tokio_postgres::types::Type::from_oid()`.
///
/// The constants in this module use the **internal `pg_type.typname` values** (e.g. `"oradate"`,
/// `"oravarcharchar"`), not the user-visible SQL type names (e.g. `DATE`, `VARCHAR2`). This is
/// what `typ.name()` returns after `resolve_type` queries `pg_type`.
///
/// This module provides:
/// - Type name constants matching `pg_type.typname` in IvorySQL.
/// - Arrow DataType mappings for those types.
/// - A helper to identify which types need `ivory_original_type` field metadata (currently only
///   Oracle DATE, whose Arrow representation — Timestamp — differs semantically from the name).
use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use arrow_schema::{DECIMAL128_MAX_PRECISION, DECIMAL_DEFAULT_SCALE};
use tokio_postgres::types::Type;

// ── Date / time types ─────────────────────────────────────────────────────────

/// Oracle DATE (含时间：年月日时分秒，无小数秒，无时区). pg_type.typname = "oradate"
pub const IVORY_DATE: &str = "oradate";

/// Oracle TIMESTAMP[(n)] (无时区，最大 9 位小数秒). pg_type.typname = "oratimestamp"
pub const IVORY_TIMESTAMP: &str = "oratimestamp";

/// Oracle TIMESTAMP[(n)] WITH TIME ZONE. pg_type.typname = "oratimestamptz"
pub const IVORY_TIMESTAMPTZ: &str = "oratimestamptz";

/// Oracle TIMESTAMP[(n)] WITH LOCAL TIME ZONE. pg_type.typname = "oratimestampltz"
pub const IVORY_TIMESTAMPLTZ: &str = "oratimestampltz";

// ── Interval types ────────────────────────────────────────────────────────────

/// Oracle INTERVAL YEAR[(n)] TO MONTH. pg_type.typname = "yminterval"
pub const IVORY_YMINTERVAL: &str = "yminterval";

/// Oracle INTERVAL DAY[(n)] TO SECOND[(n)]. pg_type.typname = "dsinterval"
pub const IVORY_DSINTERVAL: &str = "dsinterval";

// ── Numeric types ─────────────────────────────────────────────────────────────

/// Oracle NUMBER(p,s) / NUMBER (无精度约束). pg_type.typname = "number"
pub const IVORY_NUMBER: &str = "number";

/// Oracle BINARY_FLOAT (IEEE 754 单精度). pg_type.typname = "binary_float"
pub const IVORY_BINARY_FLOAT: &str = "binary_float";

/// Oracle BINARY_DOUBLE (IEEE 754 双精度). pg_type.typname = "binary_double"
pub const IVORY_BINARY_DOUBLE: &str = "binary_double";

// ── Character types ───────────────────────────────────────────────────────────

/// Oracle VARCHAR2(n CHAR). pg_type.typname = "oravarcharchar"
pub const IVORY_VARCHAR2_CHAR: &str = "oravarcharchar";

/// Oracle VARCHAR2(n BYTE). pg_type.typname = "oravarcharbyte"
pub const IVORY_VARCHAR2_BYTE: &str = "oravarcharbyte";

/// Oracle CHAR(n CHAR). pg_type.typname = "oracharchar"
pub const IVORY_CHAR_CHAR: &str = "oracharchar";

/// Oracle CHAR(n BYTE). pg_type.typname = "oracharbyte"
pub const IVORY_CHAR_BYTE: &str = "oracharbyte";

// ── Binary type ───────────────────────────────────────────────────────────────

/// Oracle RAW(n). pg_type.typname = "raw"
pub const IVORY_RAW: &str = "raw";

/// Oracle LONG RAW. pg_type.typname = "long_raw"
pub const IVORY_LONG_RAW: &str = "long_raw";

// ── XML type ──────────────────────────────────────────────────────────────────

/// Oracle XMLTYPE. pg_type.typname = "xmltype"
pub const IVORY_XMLTYPE: &str = "xmltype";

// ── Schema / field metadata keys ─────────────────────────────────────────────

/// The PostgreSQL schema name under which IvorySQL registers Oracle-mode types.
pub const IVORY_SCHEMA: &str = "sys";

/// Arrow schema-level metadata key marking the source dialect.
pub const SCHEMA_META_SOURCE_DIALECT: &str = "source_dialect";

/// Value for `SCHEMA_META_SOURCE_DIALECT` when the table comes from IvorySQL Oracle mode.
pub const SOURCE_DIALECT_IVORYQL_ORACLE: &str = "ivorySQL_oracle";

/// Field-level metadata key storing the original IvorySQL type name for types whose Arrow
/// representation differs semantically (currently only Oracle DATE).
pub const FIELD_META_IVORY_ORIGINAL_TYPE: &str = "ivory_original_type";

/// Returns `true` if `type_name` (from `pg_type.typname`) is a known IvorySQL Oracle-mode type name.
/// Prefer [`is_ivory_type`] when a full `Type` is available, as it also validates the schema.
fn is_ivory_type_name(type_name: &str) -> bool {
    matches!(
        type_name,
        IVORY_DATE
            | IVORY_TIMESTAMP
            | IVORY_TIMESTAMPTZ
            | IVORY_TIMESTAMPLTZ
            | IVORY_YMINTERVAL
            | IVORY_DSINTERVAL
            | IVORY_NUMBER
            | IVORY_BINARY_FLOAT
            | IVORY_BINARY_DOUBLE
            | IVORY_VARCHAR2_CHAR
            | IVORY_VARCHAR2_BYTE
            | IVORY_CHAR_CHAR
            | IVORY_CHAR_BYTE
            | IVORY_RAW
            | IVORY_LONG_RAW
            | IVORY_XMLTYPE
    )
}

/// Returns `true` if `typ` is a known IvorySQL Oracle-mode type.
///
/// Checks both the schema (`sys`) and the internal `pg_type.typname`, preventing accidental
/// collisions with user-defined types that happen to share a name (e.g. a user type called
/// `"number"` in schema `"public"`).
pub fn is_ivory_type(typ: &Type) -> bool {
    typ.schema() == IVORY_SCHEMA && is_ivory_type_name(typ.name())
}

/// Decode PG typmod into (precision, scale) for NUMBER(p,s) / NUMERIC(p,s).
///
/// Returns `None` when no explicit precision is declared (modifier < VARHDRSZ, typically -1).
/// Encoding follows the PostgreSQL source:
/// <https://github.com/postgres/postgres/blob/4fbb46f61271f4b7f46ecad3de608fc2f4d7d80f/src/backend/utils/adt/numeric.c#L929>
pub fn numeric_precision_scale(modifier: i32) -> Option<(u8, i8)> {
    const VARHDRSZ: i32 = 4;
    if modifier < VARHDRSZ {
        return None;
    }
    let typmod = modifier - VARHDRSZ;
    let precision = ((typmod >> 16) & 0xffff) as u8;
    let raw_scale = typmod & 0x7ff;
    let scale = ((raw_scale ^ 1024) - 1024) as i8;
    Some((precision, scale))
}

/// Maps an IvorySQL Oracle-mode type name to its Arrow `DataType` and a flag indicating
/// whether the column should be force-marked nullable.
///
/// Returns `None` for unknown names so callers can fall back to `DataType::Utf8`.
pub fn to_arrow_data_type(type_name: &str, modifier: i32) -> Option<(DataType, bool)> {
    match type_name {
        // Oracle DATE: year/month/day + hour/min/sec, no fractional seconds, no timezone.
        // Must NOT map to Date32 — use Timestamp(us) instead.
        IVORY_DATE => Some((DataType::Timestamp(TimeUnit::Microsecond, None), false)),

        // Oracle TIMESTAMP (no timezone) — same Arrow type as PG TIMESTAMP.
        IVORY_TIMESTAMP => Some((DataType::Timestamp(TimeUnit::Microsecond, None), false)),

        // Oracle TIMESTAMP WITH TIME ZONE / WITH LOCAL TIME ZONE → store as UTC.
        IVORY_TIMESTAMPTZ | IVORY_TIMESTAMPLTZ => Some((
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        )),

        // Oracle INTERVAL YEAR TO MONTH → Arrow YearMonth interval (i32 months).
        IVORY_YMINTERVAL => Some((DataType::Interval(IntervalUnit::YearMonth), false)),

        // Oracle INTERVAL DAY TO SECOND → Arrow Duration(us) (i64 microseconds).
        IVORY_DSINTERVAL => Some((DataType::Duration(TimeUnit::Microsecond), false)),

        // NUMBER(p,s) → Decimal128(p, s), force-nullable (Decimal128 cannot represent NaN/Inf).
        // NUMBER without explicit precision → Decimal128(38, 10), same as PG NUMERIC without
        // precision. NaN/Inf values arrive as Cell::Numeric and become RowValue::Null downstream,
        // which is consistent with how PG NUMERIC handles them.
        IVORY_NUMBER => {
            let (precision, scale) = numeric_precision_scale(modifier)
                .unwrap_or((DECIMAL128_MAX_PRECISION, DECIMAL_DEFAULT_SCALE));
            Some((DataType::Decimal128(precision, scale), true))
        }

        IVORY_BINARY_FLOAT => Some((DataType::Float32, false)),
        IVORY_BINARY_DOUBLE => Some((DataType::Float64, false)),

        // Character types — all map to Utf8.
        IVORY_VARCHAR2_CHAR | IVORY_VARCHAR2_BYTE | IVORY_CHAR_CHAR | IVORY_CHAR_BYTE => {
            Some((DataType::Utf8, false))
        }

        // Binary type.
        IVORY_RAW | IVORY_LONG_RAW => Some((DataType::Binary, false)),

        // XML type — serialise as text.
        IVORY_XMLTYPE => Some((DataType::Utf8, false)),

        _ => None,
    }
}

/// Returns the value to store in `ivory_original_type` field metadata for types where the Arrow
/// representation differs semantically from what the type name implies.
///
/// Currently only Oracle DATE needs this: it is stored as `Timestamp(us)` in Arrow (same as
/// `oratimestamp`), yet downstream consumers (pg_duckdb) need to distinguish Oracle DATE from a
/// plain timestamp to return the correct type to users.
pub fn original_type_metadata(type_name: &str) -> Option<&'static str> {
    match type_name {
        IVORY_DATE => Some("DATE"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use tokio_postgres::types::Kind;

    fn ivory_type(name: &str) -> Type {
        Type::new(name.to_string(), 99000, Kind::Simple, IVORY_SCHEMA.to_string())
    }

    fn non_ivory_type(name: &str) -> Type {
        Type::new(name.to_string(), 99000, Kind::Simple, "public".to_string())
    }

    #[test]
    fn test_is_ivory_type_all_known() {
        for name in [
            IVORY_DATE,
            IVORY_TIMESTAMP,
            IVORY_TIMESTAMPTZ,
            IVORY_TIMESTAMPLTZ,
            IVORY_YMINTERVAL,
            IVORY_DSINTERVAL,
            IVORY_NUMBER,
            IVORY_BINARY_FLOAT,
            IVORY_BINARY_DOUBLE,
            IVORY_VARCHAR2_CHAR,
            IVORY_VARCHAR2_BYTE,
            IVORY_CHAR_CHAR,
            IVORY_CHAR_BYTE,
            IVORY_RAW,
            IVORY_LONG_RAW,
            IVORY_XMLTYPE,
        ] {
            assert!(
                is_ivory_type(&ivory_type(name)),
                "Expected is_ivory_type=true for {name}"
            );
        }
    }

    #[test]
    fn test_is_ivory_type_rejects_wrong_schema() {
        // Same type names but in "public" schema must not match — prevents collisions with
        // user-defined types that happen to share names like "number" or "xmltype".
        for name in [IVORY_NUMBER, IVORY_DATE, IVORY_XMLTYPE] {
            assert!(
                !is_ivory_type(&non_ivory_type(name)),
                "Type '{name}' in public schema must not be treated as IvorySQL"
            );
        }
    }

    #[test]
    fn test_is_ivory_type_rejects_standard_pg_and_old_names() {
        // Standard PG type names in public schema.
        for name in ["text", "integer", "timestamp", "numeric", "", "DATE"] {
            assert!(
                !is_ivory_type(&non_ivory_type(name)),
                "Expected false for PG type '{name}'"
            );
        }
        // Old SQL-level names (not pg_type.typname) — must not match even in sys schema.
        for name in ["date", "varchar2", "nvarchar2", "clob", "blob", "raw_long"] {
            assert!(
                !is_ivory_type(&ivory_type(name)),
                "SQL-level name '{name}' must not match; use internal pg_type.typname"
            );
        }
    }

    #[test]
    fn test_to_arrow_date_is_timestamp_not_date32() {
        let (dt, nullable) = to_arrow_data_type(IVORY_DATE, -1).unwrap();
        assert_eq!(
            dt,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            "Oracle DATE must map to Timestamp(us), not Date32"
        );
        assert!(!nullable);
    }

    #[test]
    fn test_to_arrow_timestamp_types() {
        let (dt, _) = to_arrow_data_type(IVORY_TIMESTAMP, -1).unwrap();
        assert_eq!(dt, DataType::Timestamp(TimeUnit::Microsecond, None));

        for name in [IVORY_TIMESTAMPTZ, IVORY_TIMESTAMPLTZ] {
            let (dt, _) = to_arrow_data_type(name, -1).unwrap();
            assert_eq!(
                dt,
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                "Expected Timestamp(us, UTC) for {name}"
            );
        }
    }

    #[test]
    fn test_to_arrow_interval_types() {
        let (dt, _) = to_arrow_data_type(IVORY_YMINTERVAL, -1).unwrap();
        assert_eq!(dt, DataType::Interval(IntervalUnit::YearMonth));

        let (dt, _) = to_arrow_data_type(IVORY_DSINTERVAL, -1).unwrap();
        assert_eq!(dt, DataType::Duration(TimeUnit::Microsecond));
    }

    #[test]
    fn test_to_arrow_number_no_modifier_maps_to_decimal128() {
        let (dt, nullable) = to_arrow_data_type(IVORY_NUMBER, -1).unwrap();
        assert_eq!(
            dt,
            DataType::Decimal128(DECIMAL128_MAX_PRECISION, DECIMAL_DEFAULT_SCALE),
            "NUMBER without precision must map to Decimal128(38,10), same as PG NUMERIC"
        );
        assert!(nullable, "NUMBER is always nullable (NaN/Inf → NULL)");
    }

    #[test]
    fn test_to_arrow_number_with_precision_scale() {
        // precision=10, scale=2: modifier = VARHDRSZ(4) + (10 << 16) | raw_scale(2)
        let modifier: i32 = 4 + (10 << 16) | 2;
        let (dt, nullable) = to_arrow_data_type(IVORY_NUMBER, modifier).unwrap();
        assert_eq!(dt, DataType::Decimal128(10, 2));
        assert!(nullable, "Decimal128 must be nullable to handle NaN/Inf");
    }

    #[test]
    fn test_to_arrow_string_types() {
        for name in [
            IVORY_VARCHAR2_CHAR,
            IVORY_VARCHAR2_BYTE,
            IVORY_CHAR_CHAR,
            IVORY_CHAR_BYTE,
            IVORY_XMLTYPE,
        ] {
            let (dt, nullable) = to_arrow_data_type(name, -1).unwrap();
            assert_eq!(dt, DataType::Utf8, "Expected Utf8 for {name}");
            assert!(!nullable);
        }
    }

    #[test]
    fn test_to_arrow_float_types() {
        let (dt, _) = to_arrow_data_type(IVORY_BINARY_FLOAT, -1).unwrap();
        assert_eq!(dt, DataType::Float32);
        let (dt, _) = to_arrow_data_type(IVORY_BINARY_DOUBLE, -1).unwrap();
        assert_eq!(dt, DataType::Float64);
    }

    #[test]
    fn test_to_arrow_binary_type() {
        for name in [IVORY_RAW, IVORY_LONG_RAW] {
            let (dt, nullable) = to_arrow_data_type(name, -1).unwrap();
            assert_eq!(dt, DataType::Binary, "Expected Binary for {name}");
            assert!(!nullable);
        }
    }

    #[test]
    fn test_to_arrow_unknown_returns_none() {
        for name in ["unknown_type", "text", "date", "varchar2", "blob", ""] {
            assert!(to_arrow_data_type(name, -1).is_none(), "Expected None for '{name}'");
        }
    }

    #[test]
    fn test_original_type_metadata_only_for_oradate() {
        assert_eq!(original_type_metadata(IVORY_DATE), Some("DATE"));

        for name in [
            IVORY_TIMESTAMP,
            IVORY_TIMESTAMPTZ,
            IVORY_TIMESTAMPLTZ,
            IVORY_YMINTERVAL,
            IVORY_DSINTERVAL,
            IVORY_NUMBER,
            IVORY_BINARY_FLOAT,
            IVORY_BINARY_DOUBLE,
            IVORY_VARCHAR2_CHAR,
            IVORY_VARCHAR2_BYTE,
            IVORY_CHAR_CHAR,
            IVORY_CHAR_BYTE,
            IVORY_RAW,
            IVORY_LONG_RAW,
            IVORY_XMLTYPE,
        ] {
            assert!(original_type_metadata(name).is_none(), "Expected None for {name}");
        }
    }
}
