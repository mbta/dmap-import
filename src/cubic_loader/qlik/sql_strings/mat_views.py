WC700_COMP_B_ADDENDUM = """
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

WC320_LATE_TAP_ADJUSTMENT = """
    DROP MATERIALIZED VIEW IF EXISTS ods.wc320_late_tap_adjustment;
    CREATE MATERIALIZED VIEW ods.wc320_late_tap_adjustment AS
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

MATERIALIZED_USE_TXNS_WA160 = """
    DROP MATERIALIZED VIEW IF EXISTS ods.materialized_use_txns_wa160;
    CREATE MATERIALIZED VIEW ods.materialized_use_txns_wa160 AS
    SELECT
        date(posting_day_key::text) as posting_date,
        date(settlement_day_key::text) as settlement_date,
        date(transit_day_key::text) as transit_date,
        date(ut.operating_day_key::text) as operating_date,
        ut.transaction_dtm,
        ut.source_inserted_dtm,
        voided_dtm,
        opd.operator_name,
        fpd.fare_prod_name,
        ut.transit_account_id,
        ut.serial_nbr,
        ut.pass_use_count,
        coalesce(ut.pass_cost, 0)::real / 100 as pass_cost,
        coalesce(ut.value_changed, 0)::real / 100 as value_changed,
        coalesce(ut.booking_prepaid_value, 0)::real / 100 as booking_prepaid_value,
        coalesce(ut.benefit_value, 0)::real / 100 as benefit_value,
        coalesce(ut.bankcard_payment_value, 0)::real / 100 as bankcard_payment_value,
        coalesce(ut.merchant_service_fee, 0)::real / 100 as merchant_service_fee,
        (
            coalesce(ut.pass_cost, 0)
            + coalesce(ut.value_changed, 0)
            + coalesce(ut.booking_prepaid_value, 0)
            + coalesce(ut.benefit_value, 0)
            + coalesce(ut.bankcard_payment_value,0)
            + coalesce(ut.merchant_service_fee, 0)
        )::real / 100 as total_use_cost,
        transaction_status_name,
        fpuld.fare_prod_users_list_name,
        trip_price_count,
        ut.bus_id,
        txnsd.txn_status_name,
        paygo_ride_count,
        ride_count,
        transaction_id,
        transfer_flag,
        transfer_sequence_nbr,
        tad.account_status_name,
        dw_transaction_id,
        ut.token_id,
        pass_id,
        ut.pg_card_id,
        mtd.media_type_name,
        purse_name,
        patron_trip_id,
        retrieval_ref_nbr,
        txnsd.successful_use_flag,
        ut.facility_id,
        tap_id,
        rtd.ride_type_name,
        calculated_fare,
        coalesce(fpd.rider_class_name, tad.rider_class_name) as rider_class_name,
        coalesce(one_account_value, 0) as one_account_value,
        coalesce(ut.restricted_purse_value, 0) as restricted_purse_value,
        coalesce(ut.refundable_purse_value, 0) as refundable_purse_value,
        coalesce(ut.uncollectible_amount, 0)::real / 100 as uncollectible_amount,
        tad.is_registered,
        coalesce(ut.discount_applied, 0)::real / 100 AS discount_amount,
        coalesce(ut.post_pay_amount, 0)::real / 100 AS post_pay_amount,
        CASE
            WHEN ut.transfer_flag = 2
                AND (ut.multi_ride_id IS NULL OR ride_count <= 1)
                THEN 'TRANSFER'
            WHEN ut.transfer_flag = 2
                AND (ut.multi_ride_id IS NOT NULL OR ride_count > 1)
                THEN 'MULTI-RIDE TRANSFER'
            WHEN ut.transfer_flag != 2
                AND (ut.multi_ride_id IS NOT NULL OR ride_count > 1)
                THEN 'MULTI-RIDE'
            ELSE NULL
        END AS transfer_or_multiride
    FROM
        ods.edw_use_transaction ut
    LEFT JOIN
        ods.edw_fare_product_dimension fpd on ut.fare_prod_key = fpd.fare_prod_key
    LEFT JOIN
        ods.edw_operator_dimension opd on ut.operator_key = opd.operator_key
    LEFT JOIN
        ods.edw_card_dimension cardd on ut.card_key = cardd.card_key
    LEFT JOIN
        ods.edw_ride_type_dimension rtd on ut.ride_type_key = rtd.ride_type_key
    LEFT JOIN
        ods.edw_txn_status_dimension txnsd on ut.txn_status_key = txnsd.txn_status_key
    LEFT JOIN
        ods.edw_media_type_dimension mtd on ut.media_type_key = mtd.media_type_key
    LEFT JOIN
        ods.edw_transit_account_dimension tad on cardd.transit_account_key = tad.transit_account_key
    LEFT JOIN
        ods.edw_fare_prod_users_list_dimension fpuld on fpuld.fare_prod_users_list_key = fpd.fare_prod_users_list_key
    ;
"""
