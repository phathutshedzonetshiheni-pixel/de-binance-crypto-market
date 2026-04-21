with source_data as (
    select
        upper(cast(symbol as string)) as symbol,
        lower(cast(`interval` as string)) as candle_interval,
        cast(open as numeric) as open_price,
        cast(high as numeric) as high_price,
        cast(low as numeric) as low_price,
        cast(close as numeric) as close_price,
        cast(volume as numeric) as volume,
        cast(open_time as int64) as open_time_ms,
        cast(close_time as int64) as close_time_ms,
        cast(ingest_time as int64) as ingest_time_ms
    from `{{ target.project }}.{{ env_var('BQ_DATASET_RAW', 'crypto_raw') }}.raw_batch_ohlc`
),
typed as (
    select
        symbol,
        candle_interval,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        timestamp_millis(open_time_ms) as open_ts,
        timestamp_millis(close_time_ms) as close_ts,
        timestamp_millis(ingest_time_ms) as ingest_ts,
        date(timestamp_millis(close_time_ms)) as close_date
    from source_data
    where symbol is not null
      and candle_interval is not null
      and open_time_ms is not null
      and close_time_ms is not null
),
deduped as (
    select *
    from typed
    qualify row_number() over (
        partition by symbol, candle_interval, open_ts
        order by ingest_ts desc
    ) = 1
)
select
    concat(symbol, '|', candle_interval, '|', cast(open_ts as string)) as symbol_interval_open_key,
    symbol,
    candle_interval,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    open_ts,
    close_ts,
    ingest_ts,
    close_date
from deduped
