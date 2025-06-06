COMP_B_ADDENDUM = """
    DROP MATERIALIZED VIEW IF EXISTS ods.wc700_comp_b_addendum;
    CREATE MATERIALIZED VIEW ods.wc700_comp_b_addendum AS
    SELECT
        ut.dw_transaction_id
        ,ut.transit_account_id
        ,ut.operating_day_key as operating_date
        ,ut.posting_day_key as posting_date
        ,ut.settlement_day_key as settlement_date
        ,ut.transaction_dtm
        ,th.transit_mode_name
        ,ut.patron_trip_id
        ,tr.fare_rule_description
        ,ut.transfer_sequence_nbr
        ,cd.bin
        ,-(tp.bankcard_value + case when tp.bankcard_payment_id is not null then tp.uncollectible_amount else 0 end + case when s.purse_load_id is not null then tp.stored_value else 0 end) as fare_revenue
        ,case when ut.transfer_sequence_nbr > 0 and ut.fare_due <> 0 then 'Transfer' else null end as extension_charge_reason
        ,ut.retrieval_ref_nbr
    FROM
        ods.edw_use_transaction ut
    JOIN ods.edw_patron_trip tr 
        ON tr.patron_trip_id = ut.patron_trip_id and tr.source = ut.source
    JOIN ods.edw_trip_payment tp 
        ON tp.patron_trip_id = ut.patron_trip_id and tp.source = ut.source and tp.trip_price_count = ut.trip_price_count
    JOIN ods.edw_card_dimension cd 
        ON cd.card_key = ut.card_key
    LEFT JOIN ods.edw_sale_transaction s 
        ON s.purse_load_id = tp.purse_load_id
    LEFT JOIN ods.edw_transaction_history th 
        ON th.dw_transaction_id = ut.dw_transaction_id
    WHERE
        s.sale_type_key = 26
        and ( 
                (tp.journal_entry_type_id <> 131 and tp.is_reversal = 1 and ut.txn_status_key in (36, 59))
                or (tp.journal_entry_type_id <> 131 and tp.is_reversal = 0 and ut.txn_status_key not in (36, 59, 50))
                or (tp.journal_entry_type_id = 131 and ut.ride_type_key = 24) 
            )
        and (
            ut.bankcard_payment_value <> 0
            or ut.uncollectible_amount <> 0
            or ut.value_changed <> 0
        )
    ;
"""

LATE_TAP_ADJUSTMENT = """
    DROP MATERIALIZED VIEW IF EXISTS ods.late_tap_adjustment;
    CREATE MATERIALIZED VIEW ods.late_tap_adjustment AS
    SELECT
        s.settlement_day_key
        ,s.operating_day_key
        ,u.patron_trip_id
        ,u.trip_price_count
        ,tp.is_reversal
        ,u.token_id
        ,s.purse_load_id
        ,-tp.stored_value::real / 100 AS uncollectible_amount
        ,tp.transaction_dtm
        ,u.transit_account_id
        ,tm.travel_mode_name
    FROM
        ods.edw_sale_transaction s
    JOIN
        ods.edw_trip_payment tp
        ON
            tp.purse_load_id = s.purse_load_id
    JOIN
        ods.edw_patron_trip t
        ON
            t.patron_trip_id = tp.patron_trip_id
    JOIN
        ods.edw_use_transaction u
        ON
            u.patron_trip_id = tp.patron_trip_id
            AND u.trip_price_count = tp.trip_price_count
    LEFT JOIN
        ods.edw_transaction_history en
        ON
            en.dw_transaction_id = t.dw_entry_txn_id
    LEFT JOIN
        ods.edw_travel_mode_dimension tm
        ON
            tm.travel_mode_id = en.travel_mode_id
    WHERE
        s.sale_type_key = 26
        AND u.value_changed <> 0
        AND
        (
            (
                tp.je_is_fare_adjustment = 0
                AND u.ride_type_key <> 24
                AND tp.is_reversal = 1
                AND u.fare_due < 0
            )
            OR
            (
                tp.je_is_fare_adjustment = 0
                AND u.ride_type_key <> 24
                AND tp.is_reversal = 0
                AND u.fare_due > 0
            )
            OR (tp.je_is_fare_adjustment = 1 AND u.ride_type_key = 24)
        )
    ORDER BY
        s.operating_day_key desc
        ,s.settlement_day_key desc
"""
