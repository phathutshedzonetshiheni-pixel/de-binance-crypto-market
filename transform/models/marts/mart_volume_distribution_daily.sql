with daily_symbol_volume as (
    select
        close_date as trade_date,
        symbol,
        sum(volume) as total_volume
    from {{ ref('stg_raw_batch_ohlc') }}
    where candle_interval = '{{ env_var("BATCH_INTERVAL", "1h") }}'
    group by 1, 2
),
daily_total_volume as (
    select
        trade_date,
        sum(total_volume) as market_total_volume
    from daily_symbol_volume
    group by 1
)
select
    concat(cast(dsv.trade_date as string), '|', dsv.symbol) as trade_date_symbol_key,
    dsv.trade_date,
    dsv.symbol,
    dsv.total_volume,
    dtv.market_total_volume,
    safe_divide(dsv.total_volume, nullif(dtv.market_total_volume, 0)) as volume_share
from daily_symbol_volume as dsv
inner join daily_total_volume as dtv
    on dsv.trade_date = dtv.trade_date
