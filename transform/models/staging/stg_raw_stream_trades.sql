with source_data as (
    select
        upper(cast(symbol as string)) as symbol,
        cast(trade_id as int64) as trade_id,
        cast(price as numeric) as price,
        cast(qty as numeric) as qty,
        cast(event_time as int64) as event_time_ms,
        coalesce(cast(ingest_time as int64), cast(event_time as int64)) as ingest_time_ms
    from `{{ target.project }}.{{ env_var('BQ_DATASET_RAW', 'crypto_raw') }}.raw_stream_trades`
),
typed as (
    select
        symbol,
        trade_id,
        price,
        qty,
        timestamp_millis(event_time_ms) as event_ts,
        timestamp_millis(ingest_time_ms) as ingest_ts,
        date(timestamp_millis(event_time_ms)) as event_date,
        timestamp_trunc(timestamp_millis(event_time_ms), hour) as event_hour
    from source_data
    where symbol is not null
      and trade_id is not null
      and event_time_ms is not null
),
deduped as (
    select *
    from typed
    qualify row_number() over (
        partition by symbol, trade_id
        order by ingest_ts desc, event_ts desc
    ) = 1
)
select
    concat(symbol, '|', cast(trade_id as string)) as symbol_trade_key,
    symbol,
    trade_id,
    price,
    qty,
    event_ts,
    ingest_ts,
    event_date,
    event_hour
from deduped
