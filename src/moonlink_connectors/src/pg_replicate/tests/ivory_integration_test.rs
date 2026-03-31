//! Integration tests for IvorySQL Oracle-compatible type handling.
//!
//! Run with:
//!   DATABASE_URL="postgresql://tz@localhost:5455/postgres?sslmode=disable" \
//!   cargo test -p moonlink_connectors --features connector-pg ivory_integration -- --nocapture

#![cfg(feature = "connector-pg")]

use super::test_utils::{
    create_publication_for_table, create_replication_client_and_slot, database_url,
    fetch_table_schema, set_replica_identity_full, setup_connection, spawn_sql_executor,
    TestResources,
};
use crate::pg_replicate::conversions::cdc_event::CdcEvent;
use crate::pg_replicate::conversions::Cell;
use crate::pg_replicate::ivoryql_types;
use crate::pg_replicate::postgres_source::{CdcStreamConfig, PostgresSource};
use crate::pg_replicate::util::postgres_schema_to_moonlink_schema;
use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use arrow_schema::DECIMAL128_MAX_PRECISION;
use futures::StreamExt;
use serial_test::serial;
use std::time::Duration;

const STREAM_NEXT_TIMEOUT_MS: u64 = 200;
const EVENT_COLLECTION_SECS: u64 = 5;

/// Collect CDC Insert events until `expected_count` is reached or timeout.
async fn collect_insert_cells(
    stream: &mut std::pin::Pin<
        Box<crate::pg_replicate::postgres_source::CdcStream>,
    >,
    expected_count: usize,
) -> Vec<Vec<Cell>> {
    let mut results = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(EVENT_COLLECTION_SECS);
    while results.len() < expected_count && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(
            Duration::from_millis(STREAM_NEXT_TIMEOUT_MS),
            stream.next(),
        )
        .await
        {
            Ok(Some(Ok(CdcEvent::Insert((_, row, _))))) => results.push(row.values),
            Ok(Some(Ok(_))) => {}
            _ => {}
        }
    }
    results
}

// ── Schema resolution ────────────────────────────────────────────────────────

/// Enable IvorySQL Oracle-compatibility mode on a client connection.
async fn set_oracle_mode(client: &tokio_postgres::Client) {
    client
        .simple_query("SET ivorysql.compatible_mode TO oracle;")
        .await
        .expect("failed to enable Oracle compatibility mode");
}

/// Verify that Oracle-mode types are correctly resolved from pg_type into Arrow schema.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_ivory_integration_schema_resolution() {
    let client = setup_connection().await;
    set_oracle_mode(&client).await;
    let table_name = "ivory_schema_integ";
    let publication = "ivory_schema_integ_pub";
    let slot_name = "ivory_schema_integ_slot";

    let mut resources = TestResources::new(client);
    resources.add_table(table_name.to_string());
    resources.add_publication(publication.to_string());
    resources.add_slot(slot_name.to_string());

    resources
        .client()
        .simple_query(&format!(
            "CREATE TABLE {table_name} (
                id          INTEGER PRIMARY KEY,
                ora_date    DATE,
                ora_ts      TIMESTAMP,
                ora_tstz    TIMESTAMP WITH TIME ZONE,
                ora_vc2     VARCHAR2(100),
                ora_num     NUMBER(10, 2),
                ora_num_free NUMBER,
                ora_float   BINARY_FLOAT,
                ora_double  BINARY_DOUBLE,
                ora_ym      INTERVAL YEAR TO MONTH,
                ora_ds      INTERVAL DAY TO SECOND
            );"
        ))
        .await
        .unwrap();

    set_replica_identity_full(resources.client(), table_name).await;
    create_publication_for_table(resources.client(), publication, table_name).await;
    let _ = create_replication_client_and_slot(slot_name).await;

    let schema = fetch_table_schema(publication, table_name).await;
    let (arrow_schema, _) = postgres_schema_to_moonlink_schema(&schema);

    // Schema-level source_dialect metadata
    assert_eq!(
        arrow_schema
            .metadata()
            .get(ivoryql_types::SCHEMA_META_SOURCE_DIALECT),
        Some(&ivoryql_types::SOURCE_DIALECT_IVORYQL_ORACLE.to_string()),
        "source_dialect must be set for IvorySQL tables"
    );

    // Oracle DATE → Timestamp(us) with ivory_original_type=DATE
    let date_field = arrow_schema.field_with_name("ora_date").unwrap();
    assert_eq!(
        date_field.data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None),
        "Oracle DATE must map to Timestamp(us)"
    );
    assert_eq!(
        date_field
            .metadata()
            .get(ivoryql_types::FIELD_META_IVORY_ORIGINAL_TYPE),
        Some(&"DATE".to_string()),
    );

    // TIMESTAMP → Timestamp(us, None)
    assert_eq!(
        arrow_schema.field_with_name("ora_ts").unwrap().data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    // TIMESTAMP WITH TIME ZONE → Timestamp(us, UTC)
    assert_eq!(
        arrow_schema.field_with_name("ora_tstz").unwrap().data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    // VARCHAR2 → Utf8
    assert_eq!(
        arrow_schema.field_with_name("ora_vc2").unwrap().data_type(),
        &DataType::Utf8
    );
    // NUMBER(10,2) → Decimal128(10,2), nullable
    let num_field = arrow_schema.field_with_name("ora_num").unwrap();
    assert_eq!(num_field.data_type(), &DataType::Decimal128(10, 2));
    assert!(num_field.is_nullable());
    // NUMBER (no precision) → Decimal128(38,10), nullable
    let num_free = arrow_schema.field_with_name("ora_num_free").unwrap();
    assert_eq!(
        num_free.data_type(),
        &DataType::Decimal128(DECIMAL128_MAX_PRECISION, 10)
    );
    assert!(num_free.is_nullable());
    // BINARY_FLOAT → Float32
    assert_eq!(
        arrow_schema.field_with_name("ora_float").unwrap().data_type(),
        &DataType::Float32
    );
    // BINARY_DOUBLE → Float64
    assert_eq!(
        arrow_schema.field_with_name("ora_double").unwrap().data_type(),
        &DataType::Float64
    );
    // INTERVAL YEAR TO MONTH → Interval(YearMonth)
    assert_eq!(
        arrow_schema.field_with_name("ora_ym").unwrap().data_type(),
        &DataType::Interval(IntervalUnit::YearMonth)
    );
    // INTERVAL DAY TO SECOND → Duration(Microsecond)
    assert_eq!(
        arrow_schema.field_with_name("ora_ds").unwrap().data_type(),
        &DataType::Duration(TimeUnit::Microsecond)
    );

    println!("✓ Schema resolution: all Oracle type mappings correct");
}

// ── CDC INSERT ───────────────────────────────────────────────────────────────

/// Verify CDC INSERT events carry correctly parsed Cell values for Oracle types.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_ivory_integration_cdc_insert() {
    let client = setup_connection().await;
    set_oracle_mode(&client).await;
    let table_name = "ivory_cdc_integ";
    let publication = "ivory_cdc_integ_pub";
    let slot_name = "ivory_cdc_integ_slot";

    let mut resources = TestResources::new(client);
    resources.add_table(table_name.to_string());
    resources.add_publication(publication.to_string());
    resources.add_slot(slot_name.to_string());

    resources
        .client()
        .simple_query(&format!(
            "CREATE TABLE {table_name} (
                id   INTEGER PRIMARY KEY,
                d    DATE,
                ts   TIMESTAMP,
                vc2  VARCHAR2(100),
                num  NUMBER(10, 2),
                ym   INTERVAL YEAR TO MONTH,
                ds   INTERVAL DAY TO SECOND
            );"
        ))
        .await
        .unwrap();

    set_replica_identity_full(resources.client(), table_name).await;
    create_publication_for_table(resources.client(), publication, table_name).await;

    let (replication_client, confirmed_flush_lsn) =
        create_replication_client_and_slot(slot_name).await;

    let schema = fetch_table_schema(publication, table_name).await;

    let config = CdcStreamConfig {
        publication: publication.to_string(),
        slot_name: slot_name.to_string(),
        confirmed_flush_lsn,
    };
    let mut cdc_stream = PostgresSource::create_cdc_stream(replication_client, config)
        .await
        .unwrap();
    let mut pinned = Box::pin(cdc_stream);
    pinned.as_mut().add_table_schema(schema);

    let sql_tx = spawn_sql_executor(database_url());
    resources.set_sql_tx(sql_tx.clone());

    // Enable Oracle compatibility mode before running Oracle-specific DML.
    sql_tx
        .send("SET ivorysql.compatible_mode TO oracle;".to_string())
        .unwrap();

    sql_tx
        .send(format!(
            "INSERT INTO {table_name} (id, d, ts, vc2, num, ym, ds) VALUES (
                1,
                DATE '2024-03-15',
                TIMESTAMP '2024-03-15 10:30:00.123456',
                'hello oracle',
                12345.67,
                TO_YMINTERVAL('2-3'),
                TO_DSINTERVAL('5 03:04:05.000000')
            );"
        ))
        .unwrap();

    let rows = collect_insert_cells(&mut pinned, 1).await;
    assert_eq!(rows.len(), 1, "Expected 1 CDC INSERT event");
    let row = &rows[0];

    // id=1
    assert!(matches!(row[0], Cell::I32(1)), "id mismatch: {:?}", row[0]);
    // DATE → Cell::TimeStamp
    assert!(
        matches!(row[1], Cell::TimeStamp(_)),
        "Oracle DATE must be Cell::TimeStamp, got {:?}", row[1]
    );
    // TIMESTAMP → Cell::TimeStamp
    assert!(
        matches!(row[2], Cell::TimeStamp(_)),
        "TIMESTAMP must be Cell::TimeStamp, got {:?}", row[2]
    );
    // VARCHAR2 → Cell::String
    assert!(
        matches!(&row[3], Cell::String(s) if s == "hello oracle"),
        "VARCHAR2 mismatch: {:?}", row[3]
    );
    // NUMBER → Cell::Numeric
    assert!(matches!(row[4], Cell::Numeric(_)), "NUMBER mismatch: {:?}", row[4]);
    // INTERVAL YEAR TO MONTH: 2*12+3 = 27 months
    assert!(
        matches!(row[5], Cell::I32(27)),
        "INTERVAL YEAR TO MONTH must be 27 months, got {:?}", row[5]
    );
    // INTERVAL DAY TO SECOND: 5d + 3h + 4m + 5s in µs
    let expected_us: i64 = (5 * 86_400 + 3 * 3_600 + 4 * 60 + 5) * 1_000_000;
    assert!(
        matches!(row[6], Cell::I64(v) if v == expected_us),
        "INTERVAL DAY TO SECOND expected {expected_us}, got {:?}", row[6]
    );

    println!("✓ CDC INSERT: all Oracle type Cell values correct");
}

// ── COPY (initial snapshot) ──────────────────────────────────────────────────

/// Verify COPY full-sync produces correct Cell values for Oracle types.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_ivory_integration_copy() {
    let client = setup_connection().await;
    set_oracle_mode(&client).await;
    let table_name = "ivory_copy_integ";
    let publication = "ivory_copy_integ_pub";
    let slot_name = "ivory_copy_integ_slot";

    let mut resources = TestResources::new(client);
    resources.add_table(table_name.to_string());
    resources.add_publication(publication.to_string());
    resources.add_slot(slot_name.to_string());

    resources
        .client()
        .simple_query(&format!(
            "CREATE TABLE {table_name} (
                id   INTEGER PRIMARY KEY,
                d    DATE,
                vc2  VARCHAR2(50),
                num  NUMBER(8, 3),
                ym   INTERVAL YEAR TO MONTH,
                ds   INTERVAL DAY TO SECOND
            );
            INSERT INTO {table_name} (id, d, vc2, num, ym, ds) VALUES (
                1,
                DATE '2024-06-01',
                'copy test',
                999.123,
                TO_YMINTERVAL('1-6'),
                TO_DSINTERVAL('2 12:00:00.000000')
            );"
        ))
        .await
        .unwrap();

    set_replica_identity_full(resources.client(), table_name).await;
    create_publication_for_table(resources.client(), publication, table_name).await;
    let _ = create_replication_client_and_slot(slot_name).await;

    let schema = fetch_table_schema(publication, table_name).await;

    // Use a non-replication PostgresSource for COPY
    let mut copy_source = PostgresSource::new(
        &database_url(),
        None,
        None,
        false,
    )
    .await
    .unwrap();

    let (copy_stream, _lsn) = copy_source
        .get_table_copy_stream(&schema.table_name, &schema.column_schemas)
        .await
        .unwrap();

    let rows: Vec<_> = copy_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(rows.len(), 1, "Expected 1 row from COPY");
    let row = &rows[0].values;

    // id=1
    assert!(matches!(row[0], Cell::I32(1)));
    // DATE → Cell::TimeStamp
    assert!(
        matches!(row[1], Cell::TimeStamp(_)),
        "Oracle DATE in COPY must be Cell::TimeStamp, got {:?}", row[1]
    );
    // VARCHAR2 → Cell::String
    assert!(
        matches!(&row[2], Cell::String(s) if s == "copy test"),
        "VARCHAR2 mismatch: {:?}", row[2]
    );
    // NUMBER(8,3) → Cell::Numeric
    assert!(matches!(row[3], Cell::Numeric(_)), "NUMBER mismatch: {:?}", row[3]);
    // INTERVAL YEAR TO MONTH: 1*12+6 = 18 months
    assert!(
        matches!(row[4], Cell::I32(18)),
        "INTERVAL '1-6' must be 18 months, got {:?}", row[4]
    );
    // INTERVAL DAY TO SECOND: 2d + 12h in µs
    let expected_us: i64 = (2 * 86_400 + 12 * 3_600) * 1_000_000;
    assert!(
        matches!(row[5], Cell::I64(v) if v == expected_us),
        "INTERVAL DAY TO SECOND expected {expected_us}, got {:?}", row[5]
    );

    println!("✓ COPY: all Oracle type Cell values correct");
}
