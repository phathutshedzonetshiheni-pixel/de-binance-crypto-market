with hourly_prices as (
    select
        symbol,
        event_hour,
        avg(price) as avg_price
    from {{ ref('stg_raw_stream_trades') }}
    group by 1, 2
),
hourly_returns as (
    select
        symbol,
        event_hour,
        avg_price,
        safe_divide(
            avg_price - lag(avg_price) over (
                partition by symbol
                order by event_hour
            ),
            lag(avg_price) over (
                partition by symbol
                order by event_hour
            )
        ) as hourly_return
    from hourly_prices
),
volatility_windows as (
    select
        symbol,
        event_hour,
        avg_price,
        hourly_return,
        stddev_samp(hourly_return) over (
            partition by symbol
            order by event_hour
            rows between 23 preceding and current row
        ) as volatility_24h
    from hourly_returns
)
select
    concat(symbol, '|', cast(event_hour as string)) as symbol_event_hour_key,
    symbol,
    event_hour,
    avg_price,
    hourly_return,
    volatility_24h
from volatility_windows
