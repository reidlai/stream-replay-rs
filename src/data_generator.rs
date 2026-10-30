use crate::error::ReplayError;
use arrow::array::{StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{Duration, TimeZone, Utc};
use parquet::arrow::ArrowWriter;
use std::fs::{create_dir_all, File};
use std::path::Path;
use std::sync::Arc;

pub fn generate_olist_orders_parquet(path: &Path, count: usize) -> Result<(), ReplayError> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        create_dir_all(parent)?;
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Utf8, false),
        Field::new("customer_id", DataType::Utf8, false),
        Field::new("order_status", DataType::Utf8, false),
        Field::new(
            "order_purchase_timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new(
            "order_approved_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "order_delivered_carrier_date",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "order_delivered_customer_date",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "order_estimated_delivery_date",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]));

    let mut order_ids = Vec::with_capacity(count);
    let mut customer_ids = Vec::with_capacity(count);
    let mut statuses = Vec::with_capacity(count);
    let mut purchase_ts = Vec::with_capacity(count);
    let mut approved_ts = Vec::with_capacity(count);
    let mut carrier_ts = Vec::with_capacity(count);
    let mut customer_deliv_ts = Vec::with_capacity(count);
    let mut estimated_deliv_ts = Vec::with_capacity(count);

    let base_time = Utc.with_ymd_and_hms(2017, 1, 1, 10, 0, 0).unwrap();
    let status_options = ["delivered", "shipped", "processing", "invoiced", "canceled"];

    for i in 0..count {
        let order_id = format!("olist_ord_{:06x}", 0x100000 + i);
        let customer_id = format!("olist_cust_{:06x}", 0x200000 + (i % 250));
        let status = status_options[i % status_options.len()];

        // Spread orders out chronologically (e.g. 10 minutes apart)
        let purchase_dt = base_time + Duration::seconds((i as i64) * 600);
        let approved_dt = if status != "canceled" {
            Some((purchase_dt + Duration::minutes(15)).timestamp_micros())
        } else {
            None
        };
        let carrier_dt = if status == "delivered" || status == "shipped" {
            Some((purchase_dt + Duration::hours(24)).timestamp_micros())
        } else {
            None
        };
        let customer_deliv_dt = if status == "delivered" {
            Some((purchase_dt + Duration::days(5)).timestamp_micros())
        } else {
            None
        };
        let est_dt = purchase_dt + Duration::days(12);

        order_ids.push(order_id);
        customer_ids.push(customer_id);
        statuses.push(status);

        purchase_ts.push(purchase_dt.timestamp_micros());
        approved_ts.push(approved_dt);
        carrier_ts.push(carrier_dt);
        customer_deliv_ts.push(customer_deliv_dt);
        estimated_deliv_ts.push(est_dt.timestamp_micros());
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(order_ids)),
            Arc::new(StringArray::from(customer_ids)),
            Arc::new(StringArray::from(statuses)),
            Arc::new(TimestampMicrosecondArray::from(purchase_ts)),
            Arc::new(TimestampMicrosecondArray::from(approved_ts)),
            Arc::new(TimestampMicrosecondArray::from(carrier_ts)),
            Arc::new(TimestampMicrosecondArray::from(customer_deliv_ts)),
            Arc::new(TimestampMicrosecondArray::from(estimated_deliv_ts)),
        ],
    )?;

    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

