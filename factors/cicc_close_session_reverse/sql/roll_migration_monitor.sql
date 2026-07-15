-- T 国债期货主力/连一（主力交割月 + 3 个月）移仓监控。
--
-- 阈值说明：
--   WATCH     : 连一成交量占比或持仓量占比 >= 15%
--   ACTIVE    : 连一成交量占比或持仓量占比 >= 25%
--   CROSSOVER : 连一成交量占比或持仓量占比 >= 50%
--
-- block_signal = TRUE 时，建议屏蔽当日收盘后产生、下一交易日执行的信号。

CREATE OR REPLACE VIEW v_roll_migration_monitor AS
WITH mapping_with_previous AS (
    SELECT
        trade_date,
        main_contract,
        lag(main_contract) OVER (ORDER BY trade_date) AS previous_main_contract
    FROM main_contract_mapping
),
contract_pair AS (
    SELECT
        m.trade_date,
        m.main_contract,
        m.previous_main_contract,
        c.delivery_month AS main_delivery_month,
        c.last_trade_date AS main_last_trade_date,
        strftime(
            strptime(c.delivery_month || '01', '%Y%m%d') + INTERVAL 3 MONTH,
            '%Y%m'
        ) AS next_delivery_month
    FROM mapping_with_previous AS m
    LEFT JOIN contracts AS c
        ON c.wind_code = m.main_contract
),
pair_with_next_contract AS (
    SELECT
        p.*,
        n.wind_code AS next_contract,
        n.last_trade_date AS next_last_trade_date
    FROM contract_pair AS p
    LEFT JOIN contracts AS n
        ON n.delivery_month = p.next_delivery_month
),
raw_monitor AS (
    SELECT
        p.trade_date,
        p.main_contract,
        p.next_contract,
        p.main_delivery_month,
        p.next_delivery_month,
        p.main_last_trade_date,
        p.next_last_trade_date,
        p.previous_main_contract,
        p.previous_main_contract IS NOT NULL
            AND p.previous_main_contract <> p.main_contract AS is_mapping_switch_day,
        date_diff('day', p.trade_date, p.main_last_trade_date) AS calendar_days_to_main_expiry,

        main.close AS main_close,
        main.settle AS main_settle,
        main.volume AS main_volume,
        main.oi AS main_oi,
        main.oi_chg AS main_oi_chg,

        nxt.close AS next_close,
        nxt.settle AS next_settle,
        nxt.volume AS next_volume,
        nxt.oi AS next_oi,
        nxt.oi_chg AS next_oi_chg
    FROM pair_with_next_contract AS p
    LEFT JOIN daily_bars AS main
        ON main.trade_date = p.trade_date
       AND main.wind_code = p.main_contract
    LEFT JOIN daily_bars AS nxt
        ON nxt.trade_date = p.trade_date
       AND nxt.wind_code = p.next_contract
),
monitor_metrics AS (
    SELECT
        *,
        main_volume + next_volume AS pair_volume,
        main_oi + next_oi AS pair_oi,
        next_volume / nullif(main_volume + next_volume, 0) AS next_volume_share,
        next_oi / nullif(main_oi + next_oi, 0) AS next_oi_share,
        next_volume / nullif(main_volume, 0) AS next_to_main_volume_ratio,
        next_oi / nullif(main_oi, 0) AS next_to_main_oi_ratio,
        CASE
            WHEN main_oi_chg < 0 AND next_oi_chg > 0
            THEN least(-main_oi_chg, next_oi_chg)
            ELSE 0
        END AS paired_oi_transfer,
        CASE
            WHEN main_oi_chg < 0 AND next_oi_chg > 0
            THEN least(-main_oi_chg, next_oi_chg)
                 / nullif(main_oi - main_oi_chg, 0)
            ELSE 0
        END AS paired_oi_transfer_share,
        main_volume IS NOT NULL
            AND next_volume IS NOT NULL
            AND main_oi IS NOT NULL
            AND next_oi IS NOT NULL AS data_complete
    FROM raw_monitor
),
classified AS (
    SELECT
        *,
        greatest(next_volume_share, next_oi_share) AS migration_share,
        CASE
            WHEN NOT data_complete THEN 'DATA_INCOMPLETE'
            WHEN is_mapping_switch_day THEN 'SWITCH_DAY'
            WHEN greatest(next_volume_share, next_oi_share) >= 0.50 THEN 'CROSSOVER'
            WHEN greatest(next_volume_share, next_oi_share) >= 0.25 THEN 'ACTIVE'
            WHEN greatest(next_volume_share, next_oi_share) >= 0.15 THEN 'WATCH'
            ELSE 'NORMAL'
        END AS roll_status
    FROM monitor_metrics
)
SELECT
    trade_date,
    main_contract,
    next_contract,
    main_delivery_month,
    next_delivery_month,
    main_last_trade_date,
    next_last_trade_date,
    calendar_days_to_main_expiry,
    is_mapping_switch_day,
    data_complete,
    roll_status,
    CASE
        WHEN NOT data_complete
          OR is_mapping_switch_day
          OR migration_share >= 0.25
        THEN TRUE
        ELSE FALSE
    END AS block_signal,

    main_close,
    next_close,
    main_settle,
    next_settle,
    main_volume,
    next_volume,
    pair_volume,
    round(100 * next_volume_share, 2) AS next_volume_share_pct,
    round(next_to_main_volume_ratio, 4) AS next_to_main_volume_ratio,
    main_oi,
    next_oi,
    pair_oi,
    main_oi_chg,
    next_oi_chg,
    paired_oi_transfer,
    round(100 * paired_oi_transfer_share, 2) AS paired_oi_transfer_share_pct,
    round(100 * next_oi_share, 2) AS next_oi_share_pct,
    round(next_to_main_oi_ratio, 4) AS next_to_main_oi_ratio,
    round(100 * migration_share, 2) AS migration_share_pct
FROM classified;
