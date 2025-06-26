AD_HOC_VIEW = """
    DROP VIEW IF EXISTS ods.ad_hoc_processed_taps;
    CREATE OR REPLACE VIEW ods.ad_hoc_processed_taps 
    AS
    SELECT 
        tap_id
        ,transaction_dtm
        ,device_id
        ,DEVICE_PREFIX
        ,token_id
        ,TRANSIT_ACCOUNT_ID
        ,OPERATOR_ID
        ,TRANSACTION_ID
        ,tap_status_id
        ,tap_status_desc
        ,unmatched_flag
        ,trip_id
        ,sector_id
        ,voidable_until_dtm
        ,dw_transaction_id
        ,source_inserted_dtm
    FROM ods.edw_abp_tap
    ;
"""

COMP_A_VIEW = """
    DROP VIEW IF EXISTS ods.wc700_comp_a;
    CREATE OR REPLACE VIEW ods.wc700_comp_a 
    AS
    SELECT 
        ps.settlement_day_key
        ,ps.operating_day_key
        ,ps.payment_type_key
        ,tcm.txn_channel_display
        ,tcm.sales_channel_display
        ,SUM(COALESCE(transit_value,0) + COALESCE(benefit_value,0) + COALESCE(bankcard_payment_value,0) + COALESCE(one_account_value,0))/100 AS stored_value
        ,SUM(COALESCE(pass_cost,0))/100 AS pass_cost
        ,SUM(COALESCE(enablement_fee,0))/100 AS enablement_fee
        ,SUM(COALESCE(transit_value,0) + COALESCE(benefit_value,0) + COALESCE(bankcard_payment_value,0) + COALESCE(one_account_value,0) + COALESCE(pass_cost,0) + COALESCE(enablement_fee,0) + COALESCE(replacement_fee, 0))/100 AS total_fare_revenue
    FROM 
        ods.edw_payment_summary ps
    JOIN 
        ods.edw_txn_channel_map tcm 
        ON 
            tcm.txn_source = ps.txn_source 
            AND tcm.sales_channel_key = ps.sales_channel_key 
            and tcm.payment_type_key = ps.payment_type_key
    WHERE 
        tcm.txn_group = 'Product Sales'
    GROUP BY
        ps.settlement_day_key
        ,ps.operating_day_key
        ,ps.payment_type_key
        ,tcm.txn_channel_display
        ,tcm.sales_channel_display
    ORDER BY
        operating_day_key desc
        ,settlement_day_key desc
    ;
"""


COMP_B_VIEW = """
    DROP VIEW IF EXISTS ods.wc700_comp_b;
    CREATE OR REPLACE VIEW ods.wc700_comp_b 
    AS
    SELECT 
        ps.settlement_day_key
        ,ps.operating_day_key
        ,ps.payment_type_key
        ,tcm.txn_channel_display
        ,tcm.sales_channel_display
        ,SUM(COALESCE(payment_value,0))/100 AS total_fare_revenue
    FROM 
        ods.edw_payment_summary ps
    JOIN 
        ods.edw_txn_channel_map tcm 
        ON 
            tcm.txn_source = ps.txn_source 
            AND tcm.sales_channel_key = ps.sales_channel_key 
            and tcm.payment_type_key = ps.payment_type_key
    WHERE 
        tcm.txn_group = 'Open Payment Trips'
    GROUP BY
        ps.settlement_day_key
        ,ps.operating_day_key
        ,ps.payment_type_key
        ,tcm.txn_channel_display
        ,tcm.sales_channel_display
    ORDER BY
        operating_day_key desc
        ,settlement_day_key desc
    ;
"""


COMP_C_VIEW = """
    DROP VIEW IF EXISTS ods.wc700_comp_c;
    CREATE VIEW ods.wc700_comp_c
    AS
    SELECT
        t.settlement_day_key
        ,t.operating_day_key
        ,rc.rider_class_name
        ,fp.fare_prod_name
        ,t.service_type_id
        ,t.fare_rule_description
        ,t.recovery_txn_type
        ,sum(t.minimum_fare_charge) / 100 AS recovery_calculation_amount
    FROM
        ods.edw_farerev_recovery_txn t
    LEFT JOIN
        ods.edw_rider_class_dimension rc
        ON
            rc.rider_class_id = t.rider_class_id
    LEFT JOIN
        ods.edw_fare_product_dimension fp
        ON
            fp.fare_prod_key = t.fare_prod_key
    WHERE
        fp.monetary_inst_type_id = 2
        AND t.minimum_fare_charge IS NOT NULL
    GROUP BY
        t.operating_day_key
        ,t.settlement_day_key
        ,t.service_type_id
        ,rc.rider_class_name
        ,fp.fare_prod_name
        ,t.fare_rule_description
        ,t.recovery_txn_type
    ORDER BY
        operating_day_key desc
        ,settlement_day_key desc
    ;
"""


COMP_D_VIEW = """
    DROP VIEW IF EXISTS ods.wc700_comp_d;
    CREATE OR REPLACE VIEW ods.wc700_comp_d 
    AS
    SELECT 
        ps.settlement_day_key
        ,ps.operating_day_key
        ,ps.payment_type_key
        ,tcm.txn_channel_display
        ,tcm.sales_channel_display
        ,rd.reason_name
        ,SUM(ps.payment_value)/100 as refund_value
    FROM
        ods.edw_payment_summary ps
    JOIN 
        ods.edw_txn_channel_map tcm 
        ON
            tcm.txn_source = ps.txn_source
            AND tcm.sales_channel_key = ps.sales_channel_key
            AND tcm.payment_type_key = ps.payment_type_key
    LEFT JOIN 
        ods.edw_reason_dimension rd 
        ON
            rd.reason_key = ps.reason_key
    WHERE
        tcm.txn_group = 'Direct Refunds Applied'
    GROUP BY
        ps.settlement_day_key
        ,ps.operating_day_key
        ,ps.payment_type_key
        ,tcm.txn_channel_display
        ,tcm.sales_channel_display
        ,rd.reason_name
    ORDER BY
        ps.operating_day_key desc
        ,ps.settlement_day_key desc
    ;
"""

WA160_VIEW = """
    DROP VIEW IF EXISTS ods.wa160;
    CREATE OR REPLACE VIEW ods.wa160 
    AS
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
        txnsd.successful_use_flag,
        ut.facility_id,
        tap_id,
        rtd.ride_type_name,
        coalesce(fpd.rider_class_name, tad.rider_class_name) as rider_class_name,
        coalesce(one_account_value, 0) as one_account_value,
        coalesce(ut.restricted_purse_value, 0) as restricted_purse_value,
        coalesce(ut.refundable_purse_value, 0) as refundable_purse_value,
        coalesce(ut.uncollectible_amount, 0)::real / 100 as uncollectible_amount
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
