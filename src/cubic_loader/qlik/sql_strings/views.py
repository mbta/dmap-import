AD_HOC_PROCESSED_TAPS_VIEW = """
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

WC700_COMP_A_VIEW = """
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


WC700_COMP_B_VIEW = """
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


WC700_COMP_C_VIEW = """
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


WC700_COMP_D_VIEW = """
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

WC321_CLEARING_HOUSE = """
    DROP VIEW IF EXISTS ods.wc_321_clearing_house;
    CREATE OR REPLACE VIEW ods.wc_321_clearing_house
    AS
    SELECT
        CCH_STAGE_CATEGORY.CATEGORY_NAME,
        EDW_CCH_AFC_TRANSACTION.DEVICE_ID,
        case
            WHEN EDW_FARE_PRODUCT_DIMENSION.FARE_PROD_NAME IS NULL
            AND EDW_CCH_AFC_TRANSACTION.SV_TRANSACTION IS NOT NULL THEN 'Transit Value'
            ELSE EDW_FARE_PRODUCT_DIMENSION.FARE_PROD_NAME
        END as fare_prod_name,
        EDW_CCH_AFC_TRANSACTION.LOAD_TYPE_ID,
        EDW_PAYMENT_TYPE_DIMENSION.PAYMENT_TYPE_NAME,
        EDW_ROUTE_DIMENSION.ROUTE_DESIGNATOR_DESC,
        EDW_CCH_AFC_TRANSACTION.TRANSACTION_DTM,
        EDW_CCH_AFC_TRANSACTION.ADMINISTRATIVE_FEE / 100 as administrative_fee,
        EDW_CCH_AFC_TRANSACTION.BONUS_ADDED / 100 as bonus_added,
        EDW_CCH_AFC_TRANSACTION.DEPOSIT / 100 as deposit,
        EDW_CCH_AFC_TRANSACTION.REPLACEMENT_FEE / 100 as replacement_fee,
        (
            CASE
                WHEN EDW_CCH_AFC_TRANSACTION.TRANSACTION_TYPE_ID = 'U'
                THEN EDW_CCH_AFC_TRANSACTION.SV_TRANSACTION + EDW_CCH_AFC_TRANSACTION.BENEFIT_VALUE
                  + EDW_CCH_AFC_TRANSACTION.BANKCARD_PAYMENT_VALUE
                WHEN EDW_CCH_AFC_TRANSACTION.TRANSACTION_TYPE_ID = 'S'
                THEN EDW_CCH_AFC_TRANSACTION.NET_VALUE
                ELSE EDW_CCH_AFC_TRANSACTION.SV_TRANSACTION
            END
        ) / 100,
        EDW_CCH_AFC_TRANSACTION.MONEY_PAID / 100 as money_paid,
        EDW_CCH_AFC_TRANSACTION.NET_MONEY_PAID / 100 as net_money_paid,
        EDW_CCH_AFC_TRANSACTION.NET_VALUE / 100 as net_value,
        EDW_CCH_AFC_TRANSACTION.OVERPAY / 100 as overpay,
        EDW_CCH_AFC_TRANSACTION.SV_TRANSACTION / 100 as sv_transaction,
        EDW_CCH_AFC_TRANSACTION.UNDERPAY / 100 as underpay,
        EDW_CCH_AFC_TRANSACTION.BENEFIT_VALUE / 100 as benefit_value,
        EDW_CCH_AFC_TRANSACTION.BANKCARD_PAYMENT_VALUE / 100 as bankcard_payment_value,
        CASE
            WHEN (
                EDW_CCH_AFC_TRANSACTION.TRANSACTION_TYPE_ID = 'U'
                AND EDW_CCH_AFC_TRANSACTION.PASS_USE_COUNT = 1
            )
            OR EDW_CCH_AFC_TRANSACTION.TRANSACTION_TYPE_ID IN ('S', 'O', 'M')
            THEN EDW_CCH_AFC_TRANSACTION.PASS_COST / 100
            ELSE 0
        END,
        EDW_CCH_AFC_TRANSACTION.TRANSACTION_ID,
        EDW_CCH_AFC_TRANSACTION.DW_TRANSACTION_ID,
        EDW_FARE_PRODUCT_DIMENSION.MONETARY_INST_TYPE_NAME,
        EDW_CCH_AFC_TRANSACTION.SHIPPING_FEE / 100 as shipping_fee,
        FINANCIALLY_RESP_BE_DIMENSION.BUSINESS_ENTITY_NAME AS FR_BUSINESS_ENTITY_NAME,
        EDW_BUSINESS_ENTITY_DIMENSION.BUSINESS_ENTITY_NAME,
        EDW_CCH_AFC_TRANSACTION.PAYMENT_AMOUNT / 100 as payment_amount,
        CCH_STAGE_CATEGORY.TRANSACTION_CATEGORY,
        COALESCE(EDW_CCH_AFC_TRANSACTION.REFUND_FEE / 100, 0) as refund_fee,
        EDW_CCH_AFC_TRANSACTION.PASS_LOAD_AMT_PAID_BY_BV / 100 as pass_load_amt_paid_by_bv,
        EDW_CCH_AFC_TRANSACTION.PASS_LOAD_AMT_NOT_PAID_BY_BV / 100 as pass_load_amt_not_paid_by_bv,
        EDW_SALE_TYPE_DIMENSION.SALE_TYPE_NAME,
        EDW_CCH_AFC_TRANSACTION.ONE_ACCOUNT_VALUE / 100 as one_account_value,
        EDW_CCH_AFC_TRANSACTION.UNCOLLECTIBLE_AMOUNT / 100 as uncollectible_amount,
        CCH_STAGE_REPROCESS_ACTION.SHORT_DESC,
        EDW_CCH_AFC_TRANSACTION.USER_REF_1,
        EDW_CCH_AFC_TRANSACTION.USER_REF_2,
        EDW_CCH_AFC_TRANSACTION.USER_REF_3,
        EDW_CCH_AFC_TRANSACTION.MERCHANT_NUMBER,
        EDW_CREDIT_CARD_TYPE_DIMENSION.CREDIT_CARD_TYPE_NAME,
        EDW_CCH_AFC_TRANSACTION.CAPTURE_DATE,
        EDW_CCH_AFC_TRANSACTION.BANK_SETTLEMENT_DATE,
        EDW_TRANSACTION_ORIGIN_DIMENSION.TRANSACTION_ORIGIN_NAME,
        CCH_STAGE_CATEGORIZATION_RULE.RULE_ID,
        CCH_STAGE_CATEGORIZATION_RULE.RULE_NAME,
        CCH_STAGE_TRANSACTION_TYPE.DESCRIPTION,
        COALESCE(EDW_CCH_AFC_TRANSACTION.CARD_FEE / 100, 0) as card_fee,
        EDW_CCH_AFC_TRANSACTION.PASS_USE_COUNT,
        EDW_CCH_AFC_TRANSACTION.ACTIVITY_TYPE_ID,
        EDW_CCH_AFC_TRANSACTION.BOOKING_PREPAID_VALUE / 100 as booking_prepaid_value,
        EDW_CCH_AFC_TRANSACTION.TRANSACTION_REF_NBR,
        EDW_MEDIA_TYPE_DIMENSION.MEDIA_TYPE_NAME,
        EDW_RIDER_CLASS_DIMENSION.RIDER_CLASS_NAME,
        CCH_STAGE_TRANSACTION_TYPE.TRANSACTION_TYPE_ID,
        COALESCE(EDW_CCH_AFC_TRANSACTION.DISCOUNT_APPLIED / 100, 0) AS discount_applied,
        EDW_PURSE_TYPE_DIMENSION.PURSE_NAME,
        EDW_CCH_AFC_TRANSACTION.RESTRICTED_PURSE_VALUE / 100 as restricted_purse_value,
        EDW_CCH_AFC_TRANSACTION.RETRIEVAL_REF_NBR,
        EDW_CCH_AFC_TRANSACTION.TRANSIT_ACCOUNT_ID,
        COALESCE(EDW_CCH_AFC_TRANSACTION.REFUNDABLE_PURSE_VALUE / 100, 0) as refundable_purse_value,
        EDW_CCH_AFC_TRANSACTION.ENABLEMENT_FEE / 100 as enablement_fee
    FROM
        ods.EDW_CCH_AFC_TRANSACTION
    JOIN ods.CCH_STAGE_CATEGORY
        ON CCH_STAGE_CATEGORY.TRANSACTION_CATEGORY = EDW_CCH_AFC_TRANSACTION.TRANSACTION_CATEGORY
    LEFT JOIN ods.EDW_FARE_PRODUCT_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.FARE_PROD_KEY = EDW_FARE_PRODUCT_DIMENSION.FARE_PROD_KEY
    LEFT JOIN ods.EDW_PAYMENT_TYPE_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.PAYMENT_TYPE_KEY = EDW_PAYMENT_TYPE_DIMENSION.PAYMENT_TYPE_KEY
    LEFT JOIN ods.EDW_ROUTE_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.ROUTE_KEY = EDW_ROUTE_DIMENSION.ROUTE_KEY
    LEFT JOIN ods.EDW_BUSINESS_ENTITY_DIMENSION
        ON EDW_BUSINESS_ENTITY_DIMENSION.BUSINESS_ENTITY_ID
            = EDW_CCH_AFC_TRANSACTION.BUSINESS_ENTITY_ID
    LEFT JOIN ods.EDW_BUSINESS_ENTITY_DIMENSION FINANCIALLY_RESP_BE_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.SETTLEMENT_BUSINESS_ENTITY_ID
            = FINANCIALLY_RESP_BE_DIMENSION.BUSINESS_ENTITY_KEY
    LEFT JOIN ods.EDW_SALE_TYPE_DIMENSION
        ON EDW_SALE_TYPE_DIMENSION.SALE_TYPE_KEY = EDW_CCH_AFC_TRANSACTION.SALE_TYPE_KEY
    LEFT JOIN ods.CCH_STAGE_REPROCESS_ACTION
        ON CCH_STAGE_REPROCESS_ACTION.REPROCESS_ACTION_ID
            = EDW_CCH_AFC_TRANSACTION.PROCESSING_TYPE_ID
    LEFT JOIN ods.EDW_CREDIT_CARD_TYPE_DIMENSION
        ON EDW_CREDIT_CARD_TYPE_DIMENSION.CPA_CREDIT_CARD_TYPE_ID
            = EDW_CCH_AFC_TRANSACTION.CREDIT_CARD_TYPE_ID
    LEFT JOIN ods.EDW_TRANSACTION_ORIGIN_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.TRANSACTION_ORIGIN_ID
            = EDW_TRANSACTION_ORIGIN_DIMENSION.TRANSACTION_ORIGIN_KEY
    LEFT JOIN ods.CCH_STAGE_CATEGORIZATION_RULE
        ON CCH_STAGE_CATEGORIZATION_RULE.RULE_ID = EDW_CCH_AFC_TRANSACTION.CATEGORIZATION_RULE_ID
    LEFT JOIN ods.CCH_STAGE_TRANSACTION_TYPE
        ON CCH_STAGE_TRANSACTION_TYPE.TRANSACTION_TYPE_ID
            = EDW_CCH_AFC_TRANSACTION.TRANSACTION_TYPE_ID
    LEFT JOIN ods.EDW_MEDIA_TYPE_DIMENSION
        ON EDW_MEDIA_TYPE_DIMENSION.MEDIA_TYPE_ID = EDW_CCH_AFC_TRANSACTION.MEDIA_TYPE_ID
    LEFT JOIN ods.EDW_RIDER_CLASS_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.RIDER_CLASS_ID = EDW_RIDER_CLASS_DIMENSION.RIDER_CLASS_ID
    LEFT JOIN ods.EDW_PURSE_TYPE_DIMENSION
        ON EDW_PURSE_TYPE_DIMENSION.PURSE_SKU = EDW_CCH_AFC_TRANSACTION.PURSE_SKU
    ;
"""

WC231_CLEARING_HOUSE = """
    DROP VIEW IF EXISTS ods.wc231_clearing_house;
    CREATE OR REPLACE VIEW ods.wc231_clearing_house
    AS
    SELECT
        CCH_STAGE_CATEGORY.CATEGORY_NAME,
        EDW_CCH_AFC_TRANSACTION.DEVICE_ID,
        case
            WHEN EDW_FARE_PRODUCT_DIMENSION.FARE_PROD_NAME IS NULL
            AND EDW_CCH_AFC_TRANSACTION.SV_TRANSACTION IS NOT NULL THEN 'Transit Value'
            ELSE EDW_FARE_PRODUCT_DIMENSION.FARE_PROD_NAME
        END as fare_prod_name,
        EDW_CCH_AFC_TRANSACTION.LOAD_TYPE_ID,
        EDW_PAYMENT_TYPE_DIMENSION.PAYMENT_TYPE_NAME,
        EDW_ROUTE_DIMENSION.ROUTE_DESIGNATOR_DESC,
        EDW_CCH_AFC_TRANSACTION.TRANSACTION_DTM,
        EDW_CCH_AFC_TRANSACTION.ADMINISTRATIVE_FEE / 100 as administrative_fee,
        EDW_CCH_AFC_TRANSACTION.BONUS_ADDED / 100 as bonus_added,
        EDW_CCH_AFC_TRANSACTION.DEPOSIT / 100 as deposit,
        EDW_CCH_AFC_TRANSACTION.REPLACEMENT_FEE / 100 as replacement_fee,
        (
            CASE
                WHEN EDW_CCH_AFC_TRANSACTION.TRANSACTION_TYPE_ID = 'U'
                THEN EDW_CCH_AFC_TRANSACTION.SV_TRANSACTION + EDW_CCH_AFC_TRANSACTION.BENEFIT_VALUE
                  + EDW_CCH_AFC_TRANSACTION.BANKCARD_PAYMENT_VALUE
                WHEN EDW_CCH_AFC_TRANSACTION.TRANSACTION_TYPE_ID = 'S'
                THEN EDW_CCH_AFC_TRANSACTION.NET_VALUE
                ELSE EDW_CCH_AFC_TRANSACTION.SV_TRANSACTION
            END
        ) / 100 as transaction_value,
        EDW_CCH_AFC_TRANSACTION.MONEY_PAID / 100 as money_paid,
        EDW_CCH_AFC_TRANSACTION.NET_MONEY_PAID / 100 as net_money_paid,
        EDW_CCH_AFC_TRANSACTION.NET_VALUE / 100 as net_value,
        EDW_CCH_AFC_TRANSACTION.OVERPAY / 100 as overpay,
        EDW_CCH_AFC_TRANSACTION.SV_TRANSACTION / 100 as sv_transaction,
        EDW_CCH_AFC_TRANSACTION.UNDERPAY / 100 as underpay,
        EDW_CCH_AFC_TRANSACTION.BENEFIT_VALUE / 100 as benefit_value,
        EDW_CCH_AFC_TRANSACTION.BANKCARD_PAYMENT_VALUE / 100 as bankcard_payment_value,
        CASE
            WHEN (
                EDW_CCH_AFC_TRANSACTION.TRANSACTION_TYPE_ID = 'U'
                AND EDW_CCH_AFC_TRANSACTION.PASS_USE_COUNT = 1
            )
            OR EDW_CCH_AFC_TRANSACTION.TRANSACTION_TYPE_ID IN ('S', 'O', 'M')
            THEN EDW_CCH_AFC_TRANSACTION.PASS_COST / 100
            ELSE 0
        END as pass_cost,
        EDW_CCH_AFC_TRANSACTION.TRANSACTION_ID,
        EDW_CCH_AFC_TRANSACTION.DW_TRANSACTION_ID,
        EDW_FARE_PRODUCT_DIMENSION.MONETARY_INST_TYPE_NAME,
        EDW_CCH_AFC_TRANSACTION.SHIPPING_FEE / 100 as shipping_fee,
        FINANCIALLY_RESP_BE_DIMENSION.BUSINESS_ENTITY_NAME AS FR_BUSINESS_ENTITY_NAME,
        EDW_BUSINESS_ENTITY_DIMENSION.BUSINESS_ENTITY_NAME,
        EDW_CCH_AFC_TRANSACTION.PAYMENT_AMOUNT / 100 as payment_amount,
        CCH_STAGE_CATEGORY.TRANSACTION_CATEGORY,
        COALESCE(EDW_CCH_AFC_TRANSACTION.REFUND_FEE / 100, 0) as refund_fee,
        EDW_CCH_AFC_TRANSACTION.PASS_LOAD_AMT_PAID_BY_BV / 100 as pass_load_amt_paid_by_bv,
        EDW_CCH_AFC_TRANSACTION.PASS_LOAD_AMT_NOT_PAID_BY_BV / 100 as pass_load_amt_not_paid_by_bv,
        EDW_SALE_TYPE_DIMENSION.SALE_TYPE_NAME,
        EDW_CCH_AFC_TRANSACTION.ONE_ACCOUNT_VALUE / 100 as one_account_value,
        EDW_CCH_AFC_TRANSACTION.UNCOLLECTIBLE_AMOUNT / 100 as uncollectible_amount,
        CCH_STAGE_REPROCESS_ACTION.SHORT_DESC,
        EDW_CCH_AFC_TRANSACTION.USER_REF_1,
        EDW_CCH_AFC_TRANSACTION.USER_REF_2,
        EDW_CCH_AFC_TRANSACTION.USER_REF_3,
        EDW_CCH_AFC_TRANSACTION.MERCHANT_NUMBER,
        EDW_CREDIT_CARD_TYPE_DIMENSION.CREDIT_CARD_TYPE_NAME,
        EDW_CCH_AFC_TRANSACTION.CAPTURE_DATE,
        EDW_CCH_AFC_TRANSACTION.BANK_SETTLEMENT_DATE,
        EDW_TRANSACTION_ORIGIN_DIMENSION.TRANSACTION_ORIGIN_NAME,
        CCH_STAGE_CATEGORIZATION_RULE.RULE_ID,
        CCH_STAGE_CATEGORIZATION_RULE.RULE_NAME,
        CCH_STAGE_TRANSACTION_TYPE.DESCRIPTION,
        COALESCE(EDW_CCH_AFC_TRANSACTION.CARD_FEE / 100, 0) as card_fee,
        EDW_CCH_AFC_TRANSACTION.PASS_USE_COUNT,
        EDW_CCH_AFC_TRANSACTION.ACTIVITY_TYPE_ID,
        EDW_CCH_AFC_TRANSACTION.BOOKING_PREPAID_VALUE / 100 as booking_prepaid_value,
        EDW_CCH_AFC_TRANSACTION.TRANSACTION_REF_NBR,
        EDW_MEDIA_TYPE_DIMENSION.MEDIA_TYPE_NAME,
        EDW_RIDER_CLASS_DIMENSION.RIDER_CLASS_NAME,
        CCH_STAGE_TRANSACTION_TYPE.TRANSACTION_TYPE_ID,
        COALESCE(EDW_CCH_AFC_TRANSACTION.DISCOUNT_APPLIED / 100, 0) AS discount_applied,
        EDW_PURSE_TYPE_DIMENSION.PURSE_NAME,
        EDW_CCH_AFC_TRANSACTION.RESTRICTED_PURSE_VALUE / 100 as restricted_purse_value,
        EDW_CCH_AFC_TRANSACTION.RETRIEVAL_REF_NBR,
        EDW_CCH_AFC_TRANSACTION.TRANSIT_ACCOUNT_ID,
        COALESCE(EDW_CCH_AFC_TRANSACTION.REFUNDABLE_PURSE_VALUE / 100, 0) as refundable_purse_value,
        COALESCE(EDW_CCH_AFC_TRANSACTION.ROUNDING_AMOUNT / 100, 0) AS rounding_amount,
        COALESCE(EDW_CCH_AFC_TRANSACTION.PREPAID_BANKCARD_VALUE / 100, 0) AS prepaid_bankcard_value,
        COALESCE(EDW_CCH_AFC_TRANSACTION.POST_PAY_AMOUNT / 100, 0) AS post_pay_amount,
        BOARDING_STOP_DIMENSION.STOP_POINT_DISPLAY_DESC AS boarding_stop_point_display_desc,
        ALIGHTING_STOP_DIMENSION.STOP_POINT_DISPLAY_DESC AS alighting_stop_point_display_desc,
        EDW_CCH_AFC_TRANSACTION.ENABLEMENT_FEE / 100 as enablement_fee,
        EDW_CCH_AFC_TRANSACTION.PAYMENT_TRANSIT_ACCOUNT_ID,
        EDW_CCH_AFC_TRANSACTION.OPERATING_DATE,
        EDW_CCH_AFC_TRANSACTION.PASS_ID,
        EDW_CCH_AFC_TRANSACTION.ORDER_NBR,
        EDW_CCH_AFC_TRANSACTION.LINE_ITEM_NBR,
        -- ACTIVITY_TYPE_DIMENSION.SOURCE_ACTIVITY_TYPE_NAME,
        EDW_OPERATOR_DIMENSION.OPERATOR_NAME,
        EDW_CCH_AFC_TRANSACTION.USE_TYPE,
        EDW_DATE_DIMENSION.YEAR,
        EDW_DATE_DIMENSION.DTM
        -- DISTRIBUTOR_DIMENSION.ORG_NAME
    FROM
        ods.EDW_CCH_AFC_TRANSACTION
    JOIN ods.CCH_STAGE_CATEGORY
        ON CCH_STAGE_CATEGORY.TRANSACTION_CATEGORY = EDW_CCH_AFC_TRANSACTION.TRANSACTION_CATEGORY
    LEFT JOIN ods.EDW_FARE_PRODUCT_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.FARE_PROD_KEY = EDW_FARE_PRODUCT_DIMENSION.FARE_PROD_KEY
    LEFT JOIN ods.EDW_PAYMENT_TYPE_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.PAYMENT_TYPE_KEY = EDW_PAYMENT_TYPE_DIMENSION.PAYMENT_TYPE_KEY
    LEFT JOIN ods.EDW_ROUTE_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.ROUTE_KEY = EDW_ROUTE_DIMENSION.ROUTE_KEY
    LEFT JOIN ods.EDW_BUSINESS_ENTITY_DIMENSION
        ON EDW_BUSINESS_ENTITY_DIMENSION.BUSINESS_ENTITY_ID
            = EDW_CCH_AFC_TRANSACTION.BUSINESS_ENTITY_ID
    LEFT JOIN ods.EDW_BUSINESS_ENTITY_DIMENSION FINANCIALLY_RESP_BE_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.SETTLEMENT_BUSINESS_ENTITY_ID
            = FINANCIALLY_RESP_BE_DIMENSION.BUSINESS_ENTITY_KEY
    LEFT JOIN ods.EDW_SALE_TYPE_DIMENSION
        ON EDW_SALE_TYPE_DIMENSION.SALE_TYPE_KEY = EDW_CCH_AFC_TRANSACTION.SALE_TYPE_KEY
    LEFT JOIN ods.CCH_STAGE_REPROCESS_ACTION
        ON CCH_STAGE_REPROCESS_ACTION.REPROCESS_ACTION_ID
            = EDW_CCH_AFC_TRANSACTION.PROCESSING_TYPE_ID
    LEFT JOIN ods.EDW_CREDIT_CARD_TYPE_DIMENSION
        ON EDW_CREDIT_CARD_TYPE_DIMENSION.CPA_CREDIT_CARD_TYPE_ID
            = EDW_CCH_AFC_TRANSACTION.CREDIT_CARD_TYPE_ID
    LEFT JOIN ods.EDW_TRANSACTION_ORIGIN_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.TRANSACTION_ORIGIN_ID
            = EDW_TRANSACTION_ORIGIN_DIMENSION.TRANSACTION_ORIGIN_KEY
    LEFT JOIN ods.CCH_STAGE_CATEGORIZATION_RULE
        ON CCH_STAGE_CATEGORIZATION_RULE.RULE_ID = EDW_CCH_AFC_TRANSACTION.CATEGORIZATION_RULE_ID
    LEFT JOIN ods.CCH_STAGE_TRANSACTION_TYPE
        ON CCH_STAGE_TRANSACTION_TYPE.TRANSACTION_TYPE_ID
            = EDW_CCH_AFC_TRANSACTION.TRANSACTION_TYPE_ID
    LEFT JOIN ods.EDW_MEDIA_TYPE_DIMENSION
        ON EDW_MEDIA_TYPE_DIMENSION.MEDIA_TYPE_ID = EDW_CCH_AFC_TRANSACTION.MEDIA_TYPE_ID
    LEFT JOIN ods.EDW_RIDER_CLASS_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.RIDER_CLASS_ID = EDW_RIDER_CLASS_DIMENSION.RIDER_CLASS_ID
    LEFT JOIN ods.EDW_PURSE_TYPE_DIMENSION
        ON EDW_PURSE_TYPE_DIMENSION.PURSE_SKU = EDW_CCH_AFC_TRANSACTION.PURSE_SKU
    LEFT JOIN ods.EDW_OPERATOR_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.OPERATOR_ID = EDW_OPERATOR_DIMENSION.OPERATOR_ID
    LEFT JOIN ods.EDW_DATE_DIMENSION
        ON EDW_DATE_DIMENSION.DTM = EDW_CCH_AFC_TRANSACTION.SUMMARY_PERIOD
    -- LEFT JOIN ods.DISTRIBUTOR_ORDER
    --     ON DISTRIBUTOR_ORDER.DISTRIBUTOR_ORDER_KEY = EDW_CCH_AFC_TRANSACTION.TRANSACTION_ID
    -- LEFT JOIN ods.DISTRIBUTOR_DIMENSION
    --     ON DISTRIBUTOR_DIMENSION.DISTRIBUTOR_KEY = DISTRIBUTOR_ORDER.DISTRIBUTOR_KEY
    -- LEFT JOIN ods.ACTIVITY_TYPE_DIMENSION
    --    ON ACTIVITY_TYPE_DIMENSION.ACTIVITY_TYPE_KEY = EDW_CCH_AFC_TRANSACTION.ACTIVITY_TYPE_ID
    LEFT JOIN ods.EDW_STOP_POINT_DIMENSION BOARDING_STOP_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.BOARDING_STOP_KEY = BOARDING_STOP_DIMENSION.STOP_POINT_KEY
    LEFT JOIN ods.EDW_STOP_POINT_DIMENSION ALIGHTING_STOP_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.ALIGHTING_STOP_KEY = ALIGHTING_STOP_DIMENSION.STOP_POINT_KEY
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
        patron_trip_id,
        retrieval_ref_nbr,
        txnsd.successful_use_flag,
        ut.facility_id,
        tap_id,
        rtd.ride_type_name,
        calculated_fare,
        coalesce(fpd.rider_class_name, tad.rider_class_name) as rider_class_name,
        coalesce(one_account_value, 0) as one_account_value,
        coalesce(emd.external_ref, emd.customer_member_id) as external_ref,
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
        ods.edw_member_dimension emd on ut.transit_account_id = emd.transit_account_id::bigint
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

AD_HOC_JOURNAL_ENTRIES = """
    DROP VIEW IF EXISTS ods.ad_hoc_journal_entries;
    CREATE VIEW ods.ad_hoc_journal_entries
    AS
    SELECT
    accounting_date,
    voucher,
    ledger_account,
    txn_currency_debit_amount,
    txn_currency_credit_amount,
    external_account_name,
    description,
    split_part(description, '|', 1) as transaction_category,
    split_part(description, '|', 2) as apportionment_rule,
    split_part(description, '|', 4) as operating_date,
    split_part(description, '|', 5) as fare_instrument_id
    FROM
        ods.edw_fnp_general_jrnl_account_entry
    WHERE
        ledger_name = 'MBTA'
    ;
"""

# This is no longer used but is left here to support running migrations.
WC231_PASS_ID_ADHOC = """
    DROP VIEW IF EXISTS ods.wc231_pass_id_adhoc;
    CREATE VIEW ods.wc231_pass_id_adhoc
    AS
    SELECT
        CCH_STAGE_CATEGORY.CATEGORY_NAME,
        EDW_CCH_AFC_TRANSACTION.DEVICE_ID,
        case
            WHEN EDW_FARE_PRODUCT_DIMENSION.FARE_PROD_NAME IS NULL
            AND EDW_CCH_AFC_TRANSACTION.SV_TRANSACTION IS NOT NULL THEN 'Transit Value'
            ELSE EDW_FARE_PRODUCT_DIMENSION.FARE_PROD_NAME
        END as fare_prod_name,
        EDW_CCH_AFC_TRANSACTION.LOAD_TYPE_ID,
        EDW_PAYMENT_TYPE_DIMENSION.PAYMENT_TYPE_NAME,
        EDW_ROUTE_DIMENSION.ROUTE_DESIGNATOR_DESC,
        EDW_CCH_AFC_TRANSACTION.TRANSACTION_DTM,
        EDW_CCH_AFC_TRANSACTION.ADMINISTRATIVE_FEE / 100 as administrative_fee,
        EDW_CCH_AFC_TRANSACTION.BONUS_ADDED / 100 as bonus_added,
        EDW_CCH_AFC_TRANSACTION.DEPOSIT / 100 as deposit,
        EDW_CCH_AFC_TRANSACTION.REPLACEMENT_FEE / 100 as replacement_fee,
        (
            CASE
                WHEN EDW_CCH_AFC_TRANSACTION.TRANSACTION_TYPE_ID = 'U'
                THEN EDW_CCH_AFC_TRANSACTION.SV_TRANSACTION + EDW_CCH_AFC_TRANSACTION.BENEFIT_VALUE
                  + EDW_CCH_AFC_TRANSACTION.BANKCARD_PAYMENT_VALUE
                WHEN EDW_CCH_AFC_TRANSACTION.TRANSACTION_TYPE_ID = 'S'
                THEN EDW_CCH_AFC_TRANSACTION.NET_VALUE
                ELSE EDW_CCH_AFC_TRANSACTION.SV_TRANSACTION
            END
        ) / 100,
        EDW_CCH_AFC_TRANSACTION.MONEY_PAID / 100 as money_paid,
        EDW_CCH_AFC_TRANSACTION.NET_MONEY_PAID / 100 as net_money_paid,
        EDW_CCH_AFC_TRANSACTION.NET_VALUE / 100 as net_value,
        EDW_CCH_AFC_TRANSACTION.OVERPAY / 100 as overpay,
        EDW_CCH_AFC_TRANSACTION.SV_TRANSACTION / 100 as sv_transaction,
        EDW_CCH_AFC_TRANSACTION.UNDERPAY / 100 as underpay,
        EDW_CCH_AFC_TRANSACTION.BENEFIT_VALUE / 100 as benefit_value,
        EDW_CCH_AFC_TRANSACTION.BANKCARD_PAYMENT_VALUE / 100 as bankcard_payment_value,
        CASE
            WHEN (
                EDW_CCH_AFC_TRANSACTION.TRANSACTION_TYPE_ID = 'U'
                AND EDW_CCH_AFC_TRANSACTION.PASS_USE_COUNT = 1
            )
            OR EDW_CCH_AFC_TRANSACTION.TRANSACTION_TYPE_ID IN ('S', 'O', 'M')
            THEN EDW_CCH_AFC_TRANSACTION.PASS_COST / 100
            ELSE 0
        END,
        EDW_CCH_AFC_TRANSACTION.TRANSACTION_ID,
        EDW_CCH_AFC_TRANSACTION.DW_TRANSACTION_ID,
        EDW_FARE_PRODUCT_DIMENSION.MONETARY_INST_TYPE_NAME,
        EDW_CCH_AFC_TRANSACTION.SHIPPING_FEE / 100 as shipping_fee,
        FINANCIALLY_RESP_BE_DIMENSION.BUSINESS_ENTITY_NAME AS FR_BUSINESS_ENTITY_NAME,
        EDW_BUSINESS_ENTITY_DIMENSION.BUSINESS_ENTITY_NAME,
        EDW_CCH_AFC_TRANSACTION.PAYMENT_AMOUNT / 100 as payment_amount,
        CCH_STAGE_CATEGORY.TRANSACTION_CATEGORY,
        COALESCE(EDW_CCH_AFC_TRANSACTION.REFUND_FEE / 100, 0) as refund_fee,
        EDW_CCH_AFC_TRANSACTION.PASS_LOAD_AMT_PAID_BY_BV / 100 as pass_load_amt_paid_by_bv,
        EDW_CCH_AFC_TRANSACTION.PASS_LOAD_AMT_NOT_PAID_BY_BV / 100 as pass_load_amt_not_paid_by_bv,
        EDW_SALE_TYPE_DIMENSION.SALE_TYPE_NAME,
        EDW_CCH_AFC_TRANSACTION.ONE_ACCOUNT_VALUE / 100 as one_account_value,
        EDW_CCH_AFC_TRANSACTION.UNCOLLECTIBLE_AMOUNT / 100 as uncollectible_amount,
        CCH_STAGE_REPROCESS_ACTION.SHORT_DESC,
        EDW_CCH_AFC_TRANSACTION.USER_REF_1,
        EDW_CCH_AFC_TRANSACTION.USER_REF_2,
        EDW_CCH_AFC_TRANSACTION.USER_REF_3,
        EDW_CCH_AFC_TRANSACTION.MERCHANT_NUMBER,
        EDW_CREDIT_CARD_TYPE_DIMENSION.CREDIT_CARD_TYPE_NAME,
        EDW_CCH_AFC_TRANSACTION.CAPTURE_DATE,
        EDW_CCH_AFC_TRANSACTION.BANK_SETTLEMENT_DATE,
        EDW_TRANSACTION_ORIGIN_DIMENSION.TRANSACTION_ORIGIN_NAME,
        CCH_STAGE_CATEGORIZATION_RULE.RULE_ID,
        CCH_STAGE_CATEGORIZATION_RULE.RULE_NAME,
        CCH_STAGE_TRANSACTION_TYPE.DESCRIPTION,
        COALESCE(EDW_CCH_AFC_TRANSACTION.CARD_FEE / 100, 0) as card_fee,
        EDW_CCH_AFC_TRANSACTION.PASS_USE_COUNT,
        EDW_CCH_AFC_TRANSACTION.ACTIVITY_TYPE_ID,
        EDW_CCH_AFC_TRANSACTION.BOOKING_PREPAID_VALUE / 100 as booking_prepaid_value,
        EDW_CCH_AFC_TRANSACTION.TRANSACTION_REF_NBR,
        EDW_MEDIA_TYPE_DIMENSION.MEDIA_TYPE_NAME,
        EDW_RIDER_CLASS_DIMENSION.RIDER_CLASS_NAME,
        CCH_STAGE_TRANSACTION_TYPE.TRANSACTION_TYPE_ID,
        COALESCE(EDW_CCH_AFC_TRANSACTION.DISCOUNT_APPLIED / 100, 0) AS discount_applied,
        EDW_PURSE_TYPE_DIMENSION.PURSE_NAME,
        EDW_CCH_AFC_TRANSACTION.RESTRICTED_PURSE_VALUE / 100 as restricted_purse_value,
        EDW_CCH_AFC_TRANSACTION.RETRIEVAL_REF_NBR,
        EDW_CCH_AFC_TRANSACTION.TRANSIT_ACCOUNT_ID,
        ut.pass_id,
        COALESCE(EDW_CCH_AFC_TRANSACTION.REFUNDABLE_PURSE_VALUE / 100, 0) as refundable_purse_value,
        EDW_CCH_AFC_TRANSACTION.ENABLEMENT_FEE / 100 as enablement_fee
    FROM
        ods.EDW_CCH_AFC_TRANSACTION
    JOIN ods.CCH_STAGE_CATEGORY
        ON CCH_STAGE_CATEGORY.TRANSACTION_CATEGORY = EDW_CCH_AFC_TRANSACTION.TRANSACTION_CATEGORY
    LEFT JOIN ods.EDW_FARE_PRODUCT_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.FARE_PROD_KEY = EDW_FARE_PRODUCT_DIMENSION.FARE_PROD_KEY
    LEFT JOIN ods.EDW_PAYMENT_TYPE_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.PAYMENT_TYPE_KEY = EDW_PAYMENT_TYPE_DIMENSION.PAYMENT_TYPE_KEY
    LEFT JOIN ods.EDW_ROUTE_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.ROUTE_KEY = EDW_ROUTE_DIMENSION.ROUTE_KEY
    LEFT JOIN ods.EDW_BUSINESS_ENTITY_DIMENSION
        ON EDW_BUSINESS_ENTITY_DIMENSION.BUSINESS_ENTITY_ID
            = EDW_CCH_AFC_TRANSACTION.BUSINESS_ENTITY_ID
    LEFT JOIN ods.EDW_BUSINESS_ENTITY_DIMENSION FINANCIALLY_RESP_BE_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.SETTLEMENT_BUSINESS_ENTITY_ID
            = FINANCIALLY_RESP_BE_DIMENSION.BUSINESS_ENTITY_KEY
    LEFT JOIN ods.EDW_SALE_TYPE_DIMENSION
        ON EDW_SALE_TYPE_DIMENSION.SALE_TYPE_KEY = EDW_CCH_AFC_TRANSACTION.SALE_TYPE_KEY
    LEFT JOIN ods.CCH_STAGE_REPROCESS_ACTION
        ON CCH_STAGE_REPROCESS_ACTION.REPROCESS_ACTION_ID
            = EDW_CCH_AFC_TRANSACTION.PROCESSING_TYPE_ID
    LEFT JOIN ods.EDW_CREDIT_CARD_TYPE_DIMENSION
        ON EDW_CREDIT_CARD_TYPE_DIMENSION.CPA_CREDIT_CARD_TYPE_ID
            = EDW_CCH_AFC_TRANSACTION.CREDIT_CARD_TYPE_ID
    LEFT JOIN ods.EDW_TRANSACTION_ORIGIN_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.TRANSACTION_ORIGIN_ID
            = EDW_TRANSACTION_ORIGIN_DIMENSION.TRANSACTION_ORIGIN_KEY
    LEFT JOIN ods.CCH_STAGE_CATEGORIZATION_RULE
        ON CCH_STAGE_CATEGORIZATION_RULE.RULE_ID = EDW_CCH_AFC_TRANSACTION.CATEGORIZATION_RULE_ID
    LEFT JOIN ods.CCH_STAGE_TRANSACTION_TYPE
        ON CCH_STAGE_TRANSACTION_TYPE.TRANSACTION_TYPE_ID
            = EDW_CCH_AFC_TRANSACTION.TRANSACTION_TYPE_ID
    LEFT JOIN ods.EDW_MEDIA_TYPE_DIMENSION
        ON EDW_MEDIA_TYPE_DIMENSION.MEDIA_TYPE_ID = EDW_CCH_AFC_TRANSACTION.MEDIA_TYPE_ID
    LEFT JOIN ods.EDW_RIDER_CLASS_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.RIDER_CLASS_ID = EDW_RIDER_CLASS_DIMENSION.RIDER_CLASS_ID
    LEFT JOIN ods.EDW_PURSE_TYPE_DIMENSION
        ON EDW_PURSE_TYPE_DIMENSION.PURSE_SKU = EDW_CCH_AFC_TRANSACTION.PURSE_SKU
    LEFT JOIN ods.edw_use_transaction ut
        ON ut.transaction_id = EDW_CCH_AFC_TRANSACTION.transaction_id
    ;
"""

WO110 = """
DROP VIEW IF EXISTS ods.wo110;
CREATE VIEW ods.wo110
AS
WITH CSR_PATRON_ORDER_DETAIL AS (SELECT o.order_dtm AS order_date,
       TO_TIMESTAMP(CAST(py.settlement_day_key AS VARCHAR),'YYYYMMDD') AS sales_settlement_date,
       py.settlement_day_key,
       o.order_origin,
       o.device_key,
       o.merchant_id,
       o.retrieval_ref_nbr,
       o.order_nbr AS order_number,
       o.line_item_count,
       o.order_total_product_value,
       o.order_total_deposit_value,
       o.order_total_replacement_fee + o.order_total_administrative_fee + o.order_total_shipping_fee + o.order_total_card_fee + o.order_total_enablement_fee AS order_total_fees_value,
       o.order_total_value,
       py.shipping_fee,
       o.order_total_sales_tax,
       o.order_total_prepaid_amount,
       o.order_total_postpaid_amount,
       o.order_status,
       CASE ot.order_type_enum
         WHEN 'TravelCorrection' THEN li.travel_operator_id
         ELSE COALESCE(odli.operator_id, od.operator_id)
       END AS operator_id,
       CASE WHEN o.order_type = 'Refund' THEN rr.reason_name ELSE rd.reason_name END AS reason,
       o.created_by_user_id,
       o.order_completed_dtm,
       o.source,
       li.line_item_sequence,
       li.line_item_type,
       li.card_type,
       CASE
         WHEN li.card_key IS NULL AND li.subsystem_account_ref IS NOT NULL AND li.subsystem_enum IS NOT NULL THEN
           li.subsystem_account_ref||'('||SUBSTR(li.subsystem_enum,1,1)||')'
         WHEN tad.transit_account_id IS NOT NULL THEN
           CAST(tad.transit_account_id AS VARCHAR)||'(A)'
         WHEN cd.serial_nbr IS NOT NULL THEN
           cd.serial_nbr||'(C)'
         WHEN li.oa_account_id IS NOT NULL THEN
           CAST(li.oa_account_id AS VARCHAR)||'(O)'
         ELSE
           NULL
       END AS card_or_account_number,
       COALESCE(tad.transit_account_id,CAST(li.subsystem_account_ref AS BIGINT)) AS transit_account_id,
       tad.source AS transit_account_source,
       cd.serial_nbr,
       li.oa_account_id,
       CASE
         WHEN li.product_group = 'Not Applicable' THEN li.product_group
         ELSE COALESCE(fpd.fare_prod_name, li.line_item_name, li.product_group, 'Unknown')
       END AS product_description,
       li.product_group,
       ptd.payment_type_name AS payment_type,
       py.product_value,
       py.pass_cost,
       py.transit_value,
       py.benefit_value,
       py.deposit_value,
       py.replacement_fee,
       py.administrative_fee,
       py.shipping_fee AS line_item_shipping_fee,
       py.card_fee AS line_item_card_fee,
       py.sales_tax AS line_item_sales_tax,
       py.replacement_fee AS line_item_replacement_fee,
       py.enablement_fee AS line_item_enablement_fee,
       py.one_account_value AS line_item_one_account_value,
       py.prepaid_amount AS line_item_prepaid_amount,
       py.postpaid_amount AS line_item_postpaid_amount,
       py.rounding_amount AS line_item_rounding_amount,
       py.payment_amount AS line_item_total_value,
       py.discount_amount,
       py.promotion_id,
       py.promotion_sponsor_key AS promotion_sponsor_id,
       py.refund_fee,
       li.line_item_status,
       fpd.fare_prod_key,
       COALESCE(li.ticket_or_book_id, CAST(li.pass_id AS VARCHAR)) AS pass_id, --passid is BIGINT
       fpuld.fare_prod_users_list_name,
       'PII' AS patron_name,
       o.legacy_account_id AS legacy_account_number,
       o.legacy_card_nbr AS legacy_card_number,
       fpd.sku passsku,
       fpd.fare_prod_id AS fare_instrument_id,
       li.purse_type,
       li.grouping_id,
       li.line_item_nbr AS order_detail_id,
       o.order_nbr AS order_id,
       o.order_type,
       CASE WHEN o.order_type = 'Refund' THEN rr.reason_code ELSE rd.reason_code END AS reason_code,
       CASE WHEN o.order_type = 'Refund' THEN COALESCE(o.refund_reason_key, li.reason_key) ELSE o.reason_key END AS reason_key,
       o.order_day_key AS order_date_key,
       ftd.fee_type_key,
       (SELECT SUM(payment_amount) FROM ods.EDW_PATRON_ORDER_PAYMENT
         WHERE dw_patron_order_id=o.dw_patron_order_id ) AS cr_db_amount, --AND payment_type_key=c_payment_type_credit)
       o.collection_status,
       o.collection_date,
       o.submittal_count,
       CASE WHEN o.review_approved_rejected = 'Approved' THEN o.review_assigned_to_user_id ELSE NULL END AS approved_by_user_id,
       o.review_approved_dtm AS approved_dtm,
       COALESCE(o.notes, li.notes) AS adjustment_notes,
       bill.first_name AS bill_to_first_name,
       bill.last_name AS bill_to_last_name,
       py.card_fee_count AS line_item_card_fee_count,
       li.operating_day_key,
       o.sales_channel_key,
       o.approval_user_id,
       li.travel_transaction_id,
       li.travel_presence_id,
       li.line_item_name,
       cd.token_id,
       li.line_item_amount,
       li.autoload_enroll_action,
       COALESCE(li.pg_card_id, cd.pg_card_id) AS pg_card_id,
       COALESCE(fpd.rider_class_name, rc.rider_class_name, tad.rider_class_name) AS rider_class_name,
       cd.media_type_name,
       li.xfer_to_subsystem_account_ref,
       py.purse_sku,
       COALESCE(prstd.purse_name, li.subsystem_purse_restriction) AS purse_name,
       py.restricted_purse_value,
       py.refundable_purse_value,
       py.prepaid_bankcard_value,
       o.location_id,
       be.business_entity_name,
       be.address_1 AS business_entity_address_1,
       be.address_2 AS business_entity_address_2,
       be.city AS business_entity_city,
       be.state AS business_entity_state,
       be.country AS business_entity_country,
       be.postal_code AS business_entity_postal_code,
       py.ready_for_settlement_dtm,
       TO_TIMESTAMP(CAST(py.ready_for_settlement_day_key AS VARCHAR),'YYYYMMDD') AS ready_for_settlement_date,
       os.ready_for_settlement_flag,
       os.no_financial_impact_flag,
       ot.include_on_reports_flag,
       ot.send_to_cch_flag,
       ot.cch_feed_sale_type_key,
       li.dw_patron_order_id,
       li.dw_patron_order_line_item_id,
       py.dw_patron_order_payment_id,
       py.dw_transaction_id,
       py.transaction_category,
       CASE WHEN py.subsystem_enum = 'ABP' THEN py.subsystem_account_ref END AS payment_transit_account_id,
       o.employee_key,
       o.inserted_user_id,
       o.inserted_first_name,
       o.inserted_last_name,
       o.updated_origin,
       o.updated_user_id,
       o.updated_first_name,
       o.updated_last_name,
       li.line_item_refund_fee,
       li.external_order_reference,
       li.origin_stop_point_id,
       li.origin_stop_point_desc,
       li.destination_stop_point_id,
       li.destination_stop_point_desc,
       li.media_sku,
       li.issue_media_bus_trip_id,
       li.issue_media_stop_point_id,
       li.issue_media_route_id,
       CASE WHEN li.xfer_to_subsystem_account_ref IS NOT NULL THEN li.subsystem_account_ref END AS xfer_from_subsystem_acct_ref,
       li.replaced_travel_token_id,
       CASE WHEN li.replaced_travel_token_id IS NOT NULL THEN li.travel_token_id END AS replacement_travel_token_id,
       o.source_inserted_dtm,
       o.source_updated_dtm,
       o.staging_inserted_dtm,
       o.staging_updated_dtm,
       o.edw_inserted_dtm,
       o.edw_updated_dtm
  FROM ods.edw_patron_order o
       LEFT JOIN ods.edw_patron_order_line_item li ON o.dw_patron_order_id = li.dw_patron_order_id
       LEFT JOIN ods.edw_patron_order_payment py ON li.dw_patron_order_line_item_id = py.dw_patron_order_line_item_id AND li.dw_patron_order_id = py.dw_patron_order_id
       LEFT JOIN ods.edw_operator_dimension od ON o.operator_key = od.operator_key
       LEFT JOIN ods.edw_operator_dimension odli ON li.responsible_operator_key = odli.operator_key
       LEFT JOIN ods.edw_fare_product_dimension fpd ON li.fare_prod_key = fpd.fare_prod_key
       LEFT JOIN ods.edw_fare_prod_users_list_dimension fpuld ON fpd.fare_prod_users_list_key = fpuld.fare_prod_users_list_key
       LEFT JOIN ods.edw_reason_dimension rd ON o.reason_key = rd.reason_key
       LEFT JOIN ods.edw_reason_dimension rr ON COALESCE(o.refund_reason_key, li.reason_key) = rr.reason_key
       LEFT JOIN ods.edw_payment_type_dimension ptd ON py.payment_type_key = ptd.payment_type_key
       LEFT JOIN ods.edw_card_dimension cd ON li.card_key = cd.card_key
       LEFT JOIN ods.edw_transit_account_dimension tad ON cd.transit_account_key = tad.transit_account_key
       LEFT JOIN ods.edw_fee_type_dimension ftd ON li.fee_type_key = ftd.fee_type_key
       LEFT JOIN ods.edw_patron_order_status_dimension os ON os.source = o.source AND os.order_status_name = o.order_status
       LEFT JOIN ods.edw_patron_order_type_dimension ot ON ot.source = o.source AND ot.order_type_name = o.order_type
       LEFT JOIN ods.edw_contact_dimension bill ON bill.contact_key = o.bill_to_contact_key
       LEFT JOIN ods.edw_rider_class_dimension rc ON rc.rider_class_id = li.account_rider_class_id
       LEFT JOIN ods.edw_purse_type_dimension prstd ON prstd.purse_sku = li.purse_sku
       LEFT JOIN ods.edw_business_entity_dimension be ON be.business_entity_key = CAST(o.location_id AS INT) AND TRANSLATE(o.location_id, 'x0123456789', 'x') IS NULL
 WHERE (ot.include_on_reports_flag = 1 OR ot.send_to_cch_flag = 1)
   AND (   (NOT (o.order_type = 'Refund' AND o.source = 'PIVOTAL'))
        OR (o.review_approved_rejected = 'Approved' AND COALESCE(o.refund_is_opt_out,0) = 0)
       )
)
SELECT
  'WO110',
  CSR_PATRON_ORDER_DETAIL.SALES_SETTLEMENT_DATE,
  CSR_PATRON_ORDER_DETAIL.ORDER_DATE,
  CSR_PATRON_ORDER_DETAIL.ORDER_ORIGIN,
  CSR_PATRON_ORDER_DETAIL.ORDER_NUMBER,
  COALESCE(CSR_PATRON_ORDER_DETAIL.LINE_ITEM_COUNT,0) AS LINE_ITEM_COUNT,
  (CSR_PATRON_ORDER_DETAIL.ORDER_TOTAL_PRODUCT_VALUE) / 100 AS ORDER_TOTAL_PRODUCT_VALUE,
  (CSR_PATRON_ORDER_DETAIL.ORDER_TOTAL_FEES_VALUE) / 100 AS ORDER_TOTAL_FEES_VALUE,
  CSR_PATRON_ORDER_DETAIL.ORDER_TOTAL_DEPOSIT_VALUE / 100 AS ORDER_TOTAL_DEPOSIT_VALUE,
  (CSR_PATRON_ORDER_DETAIL.ORDER_TOTAL_VALUE) / 100 AS ORDER_TOTAL_VALUE,
  CSR_PATRON_ORDER_DETAIL.ORDER_STATUS,
  CASE  
WHEN ((CSR_PATRON_ORDER_DETAIL.AUTOLOAD_ENROLL_ACTION IS NOT NULL)) 
    THEN CSR_PATRON_ORDER_DETAIL.LINE_ITEM_TYPE || ' (' || CSR_PATRON_ORDER_DETAIL.AUTOLOAD_ENROLL_ACTION || ')' 
ELSE CSR_PATRON_ORDER_DETAIL.LINE_ITEM_TYPE
END AS LINE_ITEM_TYPE,
  CSR_PATRON_ORDER_DETAIL.MEDIA_TYPE_NAME,
  CAST(CSR_PATRON_ORDER_DETAIL.CARD_OR_ACCOUNT_NUMBER AS VARCHAR) AS CARD_OR_ACCOUNT_NUMBER,
  CSR_PATRON_ORDER_DETAIL.PRODUCT_DESCRIPTION,
  CSR_PATRON_ORDER_DETAIL.PAYMENT_TYPE,
  COALESCE((CSR_PATRON_ORDER_DETAIL.PRODUCT_VALUE) / 100,0) AS PRODUCT_VALUE,
  (CSR_PATRON_ORDER_DETAIL.DEPOSIT_VALUE) / 100 AS DEPOSIT_VALUE,
  (CSR_PATRON_ORDER_DETAIL.REPLACEMENT_FEE) / 100 AS REPLACEMENT_FEE,
  (CSR_PATRON_ORDER_DETAIL.ADMINISTRATIVE_FEE) / 100 AS ADMINISTRATIVE_FEE,
  (CSR_PATRON_ORDER_DETAIL.LINE_ITEM_TOTAL_VALUE) / 100 AS LINE_ITEM_TOTAL_VALUE,
  CSR_PATRON_ORDER_DETAIL.LINE_ITEM_STATUS,
  CSR_PATRON_ORDER_DETAIL.LINE_ITEM_SEQUENCE,
  CSR_PATRON_ORDER_DETAIL.LINE_ITEM_SHIPPING_FEE/100 AS LINE_ITEM_SHIPPING_FEE,
  CSR_PATRON_ORDER_DETAIL.ORDER_DETAIL_ID,
  COALESCE(CSR_PATRON_ORDER_DETAIL.REASON,'N/A') AS REASON,
  CSR_PATRON_ORDER_DETAIL.COLLECTION_STATUS,
  CSR_PATRON_ORDER_DETAIL.COLLECTION_DATE,
  CSR_PATRON_ORDER_DETAIL.SUBMITTAL_COUNT,
  COALESCE(CSR_PATRON_ORDER_DETAIL.TRANSIT_ACCOUNT_ID,0) AS TRANSIT_ACCOUNT_ID,
  CSR_PATRON_ORDER_DETAIL.LINE_ITEM_CARD_FEE/100 AS LINE_ITEM_CARD_FEE,
  CSR_PATRON_ORDER_DETAIL.LINE_ITEM_ENABLEMENT_FEE/100 AS LINE_ITEM_ENABLEMENT_FEE,
  CSR_PATRON_ORDER_DETAIL.LINE_ITEM_REPLACEMENT_FEE/100 AS LINE_ITEM_REPLACEMENT_FEE,
  CSR_PATRON_ORDER_DETAIL.PG_CARD_ID,
  CSR_PATRON_ORDER_DETAIL.RIDER_CLASS_NAME,
  COALESCE(CSR_PATRON_ORDER_DETAIL.DISCOUNT_AMOUNT/100,0) AS DISCOUNT_AMOUNT,
  CSR_PATRON_ORDER_DETAIL.BUSINESS_ENTITY_NAME	,
  CSR_PATRON_ORDER_DETAIL.BUSINESS_ENTITY_ADDRESS_1	,
  CSR_PATRON_ORDER_DETAIL.BUSINESS_ENTITY_ADDRESS_2	,
  CSR_PATRON_ORDER_DETAIL.BUSINESS_ENTITY_CITY	,
  CSR_PATRON_ORDER_DETAIL.BUSINESS_ENTITY_STATE	,
  CSR_PATRON_ORDER_DETAIL.BUSINESS_ENTITY_COUNTRY	,
  CSR_PATRON_ORDER_DETAIL.BUSINESS_ENTITY_POSTAL_CODE	,
  COALESCE(CSR_PATRON_ORDER_DETAIL.REFUND_FEE/100,0) AS REFUND_FEE,
  CSR_PATRON_ORDER_DETAIL.PASS_ID,
  CAST(DT_EMPLOYEE_DIMENSION.EMPLOYEE_SERIAL_NBR AS BIGINT) AS EMPLOYEE_SERIAL_NBR, --dont have a column named EMPLOYEE_IDENTIFICATION swapped to EMPLOYEE_SERIAL_NBR
  CSR_PATRON_ORDER_DETAIL.OA_ACCOUNT_ID,
  COALESCE(CSR_PATRON_ORDER_DETAIL.LINE_ITEM_ONE_ACCOUNT_VALUE,0)/100 AS LINE_ITEM_ONE_ACCOUNT_VALUE,
  COALESCE(CSR_PATRON_ORDER_DETAIL.line_item_rounding_amount/100,0) AS line_item_rounding_amount,
  CSR_PATRON_ORDER_DETAIL.DESTINATION_STOP_POINT_DESC,
  CSR_PATRON_ORDER_DETAIL.ORIGIN_STOP_POINT_DESC,
  COALESCE(CSR_PATRON_ORDER_DETAIL.PREPAID_BANKCARD_VALUE/100,0) AS PREPAID_BANKCARD_VALUE,
  CSR_PATRON_ORDER_DETAIL.PAYMENT_TRANSIT_ACCOUNT_ID,
  CSR_PATRON_ORDER_DETAIL.TOKEN_ID,
  CSR_PATRON_ORDER_DETAIL.SERIAL_NBR,
  TO_TIMESTAMP(CAST(CSR_PATRON_ORDER_DETAIL.OPERATING_DAY_KEY AS VARCHAR),'YYYYMMDD') AS operating_day, 
  COALESCE(od.OPERATOR_NAME,'Undefined') AS OPERATOR_NAME,
  CSR_PATRON_ORDER_DETAIL.RETRIEVAL_REF_NBR
FROM
  CSR_PATRON_ORDER_DETAIL--,
 LEFT JOIN ( SELECT * FROM ods.edw_employee_dimension) DT_EMPLOYEE_DIMENSION ON CSR_PATRON_ORDER_DETAIL.EMPLOYEE_KEY=DT_EMPLOYEE_DIMENSION.EMPLOYEE_KEY
 JOIN ods.edw_operator_dimension od ON CSR_PATRON_ORDER_DETAIL.OPERATOR_ID=od.OPERATOR_ID
 ;
"""

WO150 = """
DROP VIEW IF EXISTS ods.wo150;
CREATE VIEW ods.wo150
AS
WITH CSR_PATRON_ORDER_DETAIL AS (SELECT o.order_dtm AS order_date,
       TO_TIMESTAMP(CAST(py.settlement_day_key AS VARCHAR),'YYYYMMDD') AS sales_settlement_date,
       py.settlement_day_key,
       o.order_origin,
       o.device_key,
       o.merchant_id,
       o.retrieval_ref_nbr,
       o.order_nbr AS order_number,
       o.line_item_count,
       o.order_total_product_value,
       o.order_total_deposit_value,
       o.order_total_replacement_fee + o.order_total_administrative_fee + o.order_total_shipping_fee + o.order_total_card_fee + o.order_total_enablement_fee AS order_total_fees_value,
       o.order_total_value,
       py.shipping_fee,
       o.order_total_sales_tax,
       o.order_total_prepaid_amount,
       o.order_total_postpaid_amount,
       o.order_status,
       CASE ot.order_type_enum
         WHEN 'TravelCorrection' THEN li.travel_operator_id
         ELSE COALESCE(odli.operator_id, od.operator_id)
       END AS operator_id,
       CASE WHEN o.order_type = 'Refund' THEN rr.reason_name ELSE rd.reason_name END AS reason,
       o.created_by_user_id,
       o.order_completed_dtm,
       o.source,
       li.line_item_sequence,
       li.line_item_type,
       li.card_type,
       CASE
         WHEN li.card_key IS NULL AND li.subsystem_account_ref IS NOT NULL AND li.subsystem_enum IS NOT NULL THEN
           li.subsystem_account_ref||'('||SUBSTR(li.subsystem_enum,1,1)||')'
         WHEN tad.transit_account_id IS NOT NULL THEN
           CAST(tad.transit_account_id AS VARCHAR)||'(A)'
         WHEN cd.serial_nbr IS NOT NULL THEN
           cd.serial_nbr||'(C)'
         WHEN li.oa_account_id IS NOT NULL THEN
           CAST(li.oa_account_id AS VARCHAR)||'(O)'
         ELSE
           NULL
       END AS card_or_account_number,
       COALESCE(tad.transit_account_id,CAST(li.subsystem_account_ref AS BIGINT)) AS transit_account_id,
       tad.source AS transit_account_source,
       cd.serial_nbr,
       li.oa_account_id,
       CASE
         WHEN li.product_group = 'Not Applicable' THEN li.product_group
         ELSE COALESCE(fpd.fare_prod_name, li.line_item_name, li.product_group, 'Unknown')
       END AS product_description,
       li.product_group,
       ptd.payment_type_name AS payment_type,
       py.product_value,
       py.pass_cost,
       py.transit_value,
       py.benefit_value,
       py.deposit_value,
       py.replacement_fee,
       py.administrative_fee,
       py.shipping_fee AS line_item_shipping_fee,
       py.card_fee AS line_item_card_fee,
       py.sales_tax AS line_item_sales_tax,
       py.replacement_fee AS line_item_replacement_fee,
       py.enablement_fee AS line_item_enablement_fee,
       py.one_account_value AS line_item_one_account_value,
       py.prepaid_amount AS line_item_prepaid_amount,
       py.postpaid_amount AS line_item_postpaid_amount,
       py.rounding_amount AS line_item_rounding_amount,
       py.payment_amount AS line_item_total_value,
       py.discount_amount,
       py.promotion_id,
       py.promotion_sponsor_key AS promotion_sponsor_id,
       py.refund_fee,
       li.line_item_status,
       fpd.fare_prod_key,
       COALESCE(li.ticket_or_book_id, CAST(li.pass_id AS VARCHAR)) AS pass_id, --passid is BIGINT
       fpuld.fare_prod_users_list_name,
       'PII' AS patron_name,
       o.legacy_account_id AS legacy_account_number,
       o.legacy_card_nbr AS legacy_card_number,
       fpd.sku passsku,
       fpd.fare_prod_id AS fare_instrument_id,
       li.purse_type,
       li.grouping_id,
       li.line_item_nbr AS order_detail_id,
       o.order_nbr AS order_id,
       o.order_type,
       CASE WHEN o.order_type = 'Refund' THEN rr.reason_code ELSE rd.reason_code END AS reason_code,
       CASE WHEN o.order_type = 'Refund' THEN COALESCE(o.refund_reason_key, li.reason_key) ELSE o.reason_key END AS reason_key,
       o.order_day_key AS order_date_key,
       ftd.fee_type_key,
       (SELECT SUM(payment_amount) FROM ods.edw_patron_order_payment
         WHERE dw_patron_order_id=o.dw_patron_order_id ) AS cr_db_amount, --AND payment_type_key=c_payment_type_credit)
       o.collection_status,
       o.collection_date,
       o.submittal_count,
       CASE WHEN o.review_approved_rejected = 'Approved' THEN o.review_assigned_to_user_id ELSE NULL END AS approved_by_user_id,
       o.review_approved_dtm AS approved_dtm,
       COALESCE(o.notes, li.notes) AS adjustment_notes,
       bill.first_name AS bill_to_first_name,
       bill.last_name AS bill_to_last_name,
       py.card_fee_count AS line_item_card_fee_count,
       li.operating_day_key,
       o.sales_channel_key,
       o.approval_user_id,
       li.travel_transaction_id,
       li.travel_presence_id,
       li.line_item_name,
       cd.token_id,
       li.line_item_amount,
       li.autoload_enroll_action,
       COALESCE(li.pg_card_id, cd.pg_card_id) AS pg_card_id,
       COALESCE(fpd.rider_class_name, rc.rider_class_name, tad.rider_class_name) AS rider_class_name,
       cd.media_type_name,
       li.xfer_to_subsystem_account_ref,
       py.purse_sku,
       COALESCE(prstd.purse_name, li.subsystem_purse_restriction) AS purse_name,
       py.restricted_purse_value,
       py.refundable_purse_value,
       py.prepaid_bankcard_value,
       o.location_id,
       be.business_entity_name,
       be.address_1 AS business_entity_address_1,
       be.address_2 AS business_entity_address_2,
       be.city AS business_entity_city,
       be.state AS business_entity_state,
       be.country AS business_entity_country,
       be.postal_code AS business_entity_postal_code,
       py.ready_for_settlement_dtm,
       TO_TIMESTAMP(CAST(py.ready_for_settlement_day_key AS VARCHAR),'YYYYMMDD') AS ready_for_settlement_date,
       os.ready_for_settlement_flag,
       os.no_financial_impact_flag,
       ot.include_on_reports_flag,
       ot.send_to_cch_flag,
       ot.cch_feed_sale_type_key,
       li.dw_patron_order_id,
       li.dw_patron_order_line_item_id,
       py.dw_patron_order_payment_id,
       py.dw_transaction_id,
       py.transaction_category,
       CASE WHEN py.subsystem_enum = 'ABP' THEN py.subsystem_account_ref END AS payment_transit_account_id,
       o.employee_key,
       o.inserted_user_id,
       o.inserted_first_name,
       o.inserted_last_name,
       o.updated_origin,
       o.updated_user_id,
       o.updated_first_name,
       o.updated_last_name,
       li.line_item_refund_fee,
       li.external_order_reference,
       li.origin_stop_point_id,
       li.origin_stop_point_desc,
       li.destination_stop_point_id,
       li.destination_stop_point_desc,
       li.media_sku,
       li.issue_media_bus_trip_id,
       li.issue_media_stop_point_id,
       li.issue_media_route_id,
       CASE WHEN li.xfer_to_subsystem_account_ref IS NOT NULL THEN li.subsystem_account_ref END AS xfer_from_subsystem_acct_ref,
       li.replaced_travel_token_id,
       CASE WHEN li.replaced_travel_token_id IS NOT NULL THEN li.travel_token_id END AS replacement_travel_token_id,
       o.source_inserted_dtm,
       o.source_updated_dtm,
       o.staging_inserted_dtm,
       o.staging_updated_dtm,
       o.edw_inserted_dtm,
       o.edw_updated_dtm
  FROM ods.edw_patron_order o
       LEFT JOIN ods.edw_patron_order_line_item li ON o.dw_patron_order_id = li.dw_patron_order_id
       LEFT JOIN ods.edw_patron_order_payment py ON li.dw_patron_order_line_item_id = py.dw_patron_order_line_item_id AND li.dw_patron_order_id = py.dw_patron_order_id
       LEFT JOIN ods.edw_operator_dimension od ON o.operator_key = od.operator_key
       LEFT JOIN ods.edw_operator_dimension odli ON li.responsible_operator_key = odli.operator_key
       LEFT JOIN ods.edw_fare_product_dimension fpd ON li.fare_prod_key = fpd.fare_prod_key
       LEFT JOIN ods.edw_fare_prod_users_list_dimension fpuld ON fpd.fare_prod_users_list_key = fpuld.fare_prod_users_list_key
       LEFT JOIN ods.edw_reason_dimension rd ON o.reason_key = rd.reason_key
       LEFT JOIN ods.edw_reason_dimension rr ON COALESCE(o.refund_reason_key, li.reason_key) = rr.reason_key
       LEFT JOIN ods.edw_payment_type_dimension ptd ON py.payment_type_key = ptd.payment_type_key
       LEFT JOIN ods.edw_card_dimension cd ON li.card_key = cd.card_key
       LEFT JOIN ods.edw_transit_account_dimension tad ON cd.transit_account_key = tad.transit_account_key
       LEFT JOIN ods.edw_fee_type_dimension ftd ON li.fee_type_key = ftd.fee_type_key
       LEFT JOIN ods.edw_patron_order_status_dimension os ON os.source = o.source AND os.order_status_name = o.order_status
       LEFT JOIN ods.edw_patron_order_type_dimension ot ON ot.source = o.source AND ot.order_type_name = o.order_type
       LEFT JOIN ods.edw_contact_dimension bill ON bill.contact_key = o.bill_to_contact_key
       LEFT JOIN ods.edw_rider_class_dimension rc ON rc.rider_class_id = li.account_rider_class_id
       LEFT JOIN ods.edw_purse_type_dimension prstd ON prstd.purse_sku = li.purse_sku
       LEFT JOIN ods.edw_business_entity_dimension be ON be.business_entity_key = CAST(o.location_id AS INT) AND TRANSLATE(o.location_id, 'x0123456789', 'x') IS NULL
 WHERE (ot.include_on_reports_flag = 1 OR ot.send_to_cch_flag = 1)
   AND (   (NOT (o.order_type = 'Refund' AND o.source = 'PIVOTAL'))
        OR (o.review_approved_rejected = 'Approved' AND COALESCE(o.refund_is_opt_out,0) = 0)
       )
),
CSR_USER AS (
SELECT NULL AS USER_ID,
          NULL AS LOGIN_NAME,
          NULL AS RN_DESCRIPTOR,
          NULL AS RN_CREATE_DATE,
          NULL AS RN_CREATE_USER,
          NULL AS RN_EDIT_DATE,
          NULL AS RN_EDIT_USER,
          NULL AS EMAIL_ADDRESS,
          NULL AS LAN_SEQUENCE,
          NULL AS MOBILE_SEQUENCE,
          NULL AS KEY_PREFIX,
          NULL AS DESKTOP_BINARY,
          NULL AS DATABASE_IDENT,
          NULL AS SOURCE_DSM_USERID,
          NULL AS REPLICA_NUMBER,
          NULL AS LAST_SYNC_DATE,
          NULL AS LAST_REPLICATE_DATE,
          NULL AS LAST_CONFIGURE_DATE,
          NULL AS SOURCE_PREFIX,
          NULL AS OUTLOOK_SYNC,
          NULL AS OUTLOOK_SYNC_USER_ID,
          NULL AS OUTLOOK_SYNC_RUNNING,
          NULL AS OUTLOOK_SYNC_STARTUP,
          NULL AS LANGUAGE_ID,
          NULL AS PREFERENCES,
          NULL AS MSMQ_ADDRESS,
          NULL AS TRANSPORT,
          NULL AS REMOTEMESSAGELEVEL,
          NULL AS SEARCH_TYPE_DEFAULT,
          NULL AS FAVORITES_TREE_BINARY,
          NULL AS FAVORITES_BAR_DOCKSTATE_BINARY,
          NULL AS WAM_PREFERENCES,
          NULL AS SYNCSTREAM_INBOX_TOKEN,
          NULL AS MAXSYNCMAILSIZE,
          NULL AS CAL_PREFERENCES,
          NULL AS CAL_SETTINGS,
          NULL AS SHORTCUTS_UPDATED,
          NULL AS COMPUTER,
          NULL AS LKEY,
          NULL AS MLKEY,
          NULL AS ULD,
          NULL AS USER_TYPE,
          NULL AS IS_LOCKED,
          NULL AS DW_USERS_ID,
          NULL AS DW_INSERTED_DTM,
          NULL AS DW_UPDATED_DTM
),
CSR_EMPLOYEE AS (
		SELECT
		NULL AS	EMPLOYEE_ID,
		CAST(NULL AS VARCHAR) AS	RN_DESCRIPTOR,
		NULL AS	RN_CREATE_DATE,
		NULL AS	RN_CREATE_USER,
		NULL AS	RN_EDIT_DATE,
		NULL AS	RN_EDIT_USER,
		NULL AS	FIRST_NAME,
		NULL AS	LAST_NAME,
		NULL AS	EXTENSION,
		NULL AS	DEPARTMENT,
		NULL AS	 RN_EMPLOYEE_USER_ID,
		NULL AS	ADDRESS_1,
		NULL AS	ADDRESS_2,
		NULL AS	ADDRESS_3,
		NULL AS	COUNTRY,
		NULL AS	ZIP,
		NULL AS	WORK_PHONE,
		NULL AS	WORK_FAX,
		NULL AS	WORK_EMAIL,
		NULL AS	START_DATE,
		NULL AS	END_DATE,
		NULL AS	BIRTHDAY,
		NULL AS	GENDER,
		NULL AS	LOGIN_SCRIPT,
		NULL AS	 TERRITORY_ID,
		NULL AS	STATE_,
		NULL AS	CITY,
		NULL AS	 REGIONAL_MANAGER_ID,
		NULL AS	TITLE,
		NULL AS	HOME_PHONE,
		NULL AS	HOME_FAX,
		NULL AS	PAGER,
		NULL AS	CELL_PHONE,
		NULL AS	HOME_EMAIL,
		NULL AS	 REGION_ID,
		NULL AS	ACTIVE,
		NULL AS	JOB_TITLE,
		NULL AS	EMPLOYEE_NAME_SOUNDEX,
		NULL AS	POSSIBLE_DUPLICATE,
		NULL AS	PIN,
		NULL AS	NUMBER_,
		NULL AS	SICK_DAYS,
		NULL AS	VACATION_DAYS,
		NULL AS	PHOTO,
		NULL AS	CALL_CAPTURE_SEARCH_IN,
		NULL AS	COMMENTS,
		NULL AS	START_DATE_TEXT,
		NULL AS	WEB_EDITED,
		NULL AS	EXTERNAL_USER_NAME,
		NULL AS	REPORTS_TO,
		NULL AS	REPORTS_TO_USER_ID,
		NULL AS	SUPPORT_MARKETING_PROJECT_ID,
		NULL AS	SUPPORT_TEAM_ID,
		NULL AS	SUPPORT_ALARM_FILE,
		NULL AS	REIMBURSEMENT_CURRENCY_ID,
		NULL AS	DELTA_REPORTS_TO_ID,
		NULL AS	SALES_NOTIFICATION,
		NULL AS	ORDER_NOTIFICATION,
		NULL AS	ACTIVITY_MEETING_NOTIFICATION,
		NULL AS	ACTIVITY_GENERAL_NOTIFICATION,
		NULL AS	TIME_ZONE_ID,
		NULL AS	COMPANY_NOTIFICATION,
		NULL AS	CONTACT_NOTIFICATION,
		NULL AS	NEW_PARTNER_NOTIFICATION,
		NULL AS	TIMER_ON,
		NULL AS	DUTY_MANAGER,
		NULL AS	ESCALATION_NOTIFICATION,
		NULL AS	UPDATE_NOTIFICATION,
		NULL AS	CLIENT,
		NULL AS	UNSUBSCRIBE_FLAG,
		NULL AS	BOUNCE_COUNT,
		NULL AS	EMAIL_TYPE,
		NULL AS	LOGIN_,
		NULL AS	DW_EMPLOYEE_ID,
		NULL AS	DW_INSERTED_DTM,
		NULL AS	DW_UPDATED_DTM
)
SELECT
  'WO150',
  CSR_PATRON_ORDER_DETAIL.ORDER_STATUS,
  COALESCE(CSR_PATRON_ORDER_DETAIL.REASON,'N/A') AS reason,
  COALESCE(case when upper(CSR_PATRON_ORDER_DETAIL.PURSE_TYPE) = 'PRETAX PURSE' then 'Benefit Value' else CSR_PATRON_ORDER_DETAIL.PURSE_TYPE end,'Undefined') AS purse_type,
  COALESCE((CSR_PATRON_ORDER_DETAIL.PRODUCT_VALUE) / 100,0) AS product_value,
  COALESCE(CSR_PATRON_ORDER_DETAIL.LINE_ITEM_COUNT,0) AS line_item_count,
  COALESCE(OPERATOR_DIMENSION.OPERATOR_NAME,'Undefined') AS operator_name,
  CSR_PATRON_ORDER_DETAIL.ORDER_DATE,
  CSR_PATRON_ORDER_DETAIL.CARD_OR_ACCOUNT_NUMBER::VARCHAR AS card_or_account_number,
  CSR_PATRON_ORDER_DETAIL.ORDER_NUMBER,
  CASE WHEN 
(CSR_PATRON_ORDER_DETAIL.INSERTED_FIRST_NAME || CSR_PATRON_ORDER_DETAIL.INSERTED_LAST_NAME) IS NULL then
(CSR_PATRON_ORDER_DETAIL.INSERTED_FIRST_NAME || ' ' || CSR_PATRON_ORDER_DETAIL.INSERTED_LAST_NAME) ELSE 
(COALESCE(CSR_PATRON_ORDER_DETAIL.INSERTED_USER_ID,CSR_EMPLOYEE.FIRST_NAME || ' ' || CSR_EMPLOYEE.LAST_NAME)) END AS inserted_by
,
  CSR_PATRON_ORDER_DETAIL.APPROVED_DTM 
,
  CSR_PATRON_ORDER_DETAIL.ORDER_COMPLETED_DTM,
  FARE_PRODUCT_DIMENSION.FARE_PROD_NAME,
  DT_REASON_DIMENSION.REASON_TYPE,
  COALESCE(CSR_PATRON_ORDER_DETAIL.PASS_COST,0)/100 AS pass_cost,
  COALESCE(CSR_PATRON_ORDER_DETAIL.TRANSIT_VALUE,0)/100 AS transit_value,
  COALESCE(CSR_PATRON_ORDER_DETAIL.BENEFIT_VALUE,0)/100 AS benefit_value,
  CSR_PATRON_ORDER_DETAIL.ADJUSTMENT_NOTES,
  CSR_PATRON_ORDER_DETAIL.ORDER_ORIGIN,
  COALESCE(CSR_PATRON_ORDER_DETAIL.APPROVAL_USER_ID,CSR_EMPLOYEE_APPROVED_USER.RN_DESCRIPTOR) AS approval_user_id,
  CSR_PATRON_ORDER_DETAIL.SALES_SETTLEMENT_DATE,
  CASE  
WHEN ((CSR_PATRON_ORDER_DETAIL.AUTOLOAD_ENROLL_ACTION IS NOT NULL)) 
    THEN CSR_PATRON_ORDER_DETAIL.LINE_ITEM_TYPE || ' (' || CSR_PATRON_ORDER_DETAIL.AUTOLOAD_ENROLL_ACTION || ')' 
ELSE CSR_PATRON_ORDER_DETAIL.LINE_ITEM_TYPE
END AS line_item_type,
  CSR_PATRON_ORDER_DETAIL.PG_CARD_ID,
  CSR_PATRON_ORDER_DETAIL.RIDER_CLASS_NAME,
  CSR_PATRON_ORDER_DETAIL.MEDIA_TYPE_NAME,
  CSR_PATRON_ORDER_DETAIL.PURSE_NAME,
  COALESCE(CSR_PATRON_ORDER_DETAIL.RESTRICTED_PURSE_VALUE/100,0) AS restricted_purse_value,
    CASE
    WHEN CSR_PATRON_ORDER_DETAIL.LINE_ITEM_TYPE   IN ('Transit Account Trip Fare Adjustment', 'Transit Account Trip Void', 'Transit Account Tap Void','Transit Account Trip Correction')
    THEN (CSR_PATRON_ORDER_DETAIL.LINE_ITEM_AMOUNT /100)
    ELSE 0
  END AS line_item_amount,
  CSR_PATRON_ORDER_DETAIL.PRODUCT_DESCRIPTION,
  COALESCE(CSR_PATRON_ORDER_DETAIL.REFUNDABLE_PURSE_VALUE,0)/100 AS refundable_purse_value,
  COALESCE(CSR_PATRON_ORDER_DETAIL.PREPAID_BANKCARD_VALUE/100,0) AS prepaid_bankcard_value,
  CSR_PATRON_ORDER_DETAIL.RETRIEVAL_REF_NBR
FROM
  CSR_PATRON_ORDER_DETAIL
  LEFT JOIN ods.edw_fare_product_dimension FARE_PRODUCT_DIMENSION ON CSR_PATRON_ORDER_DETAIL.FARE_PROD_KEY = FARE_PRODUCT_DIMENSION.fare_prod_key
  LEFT JOIN CSR_USER ON CSR_PATRON_ORDER_DETAIL.CREATED_BY_USER_ID = CAST(CSR_USER.USER_ID AS NUMERIC)
  LEFT JOIN CSR_EMPLOYEE ON CSR_USER.USER_ID = CSR_EMPLOYEE.RN_EMPLOYEE_USER_ID
  JOIN ods.edw_operator_dimension OPERATOR_DIMENSION ON CSR_PATRON_ORDER_DETAIL.OPERATOR_ID=OPERATOR_DIMENSION.OPERATOR_ID
  JOIN ods.edw_date_dimension DATE_DIMENSION ON CSR_PATRON_ORDER_DETAIL.ORDER_DATE_KEY=DATE_DIMENSION.DATE_KEY
  LEFT JOIN CSR_EMPLOYEE CSR_EMPLOYEE_APPROVED_USER ON CSR_PATRON_ORDER_DETAIL.APPROVED_BY_USER_ID = CAST(CSR_EMPLOYEE_APPROVED_USER.EMPLOYEE_ID AS NUMERIC)
  LEFT JOIN (Select * from ods.edw_reason_dimension) DT_REASON_DIMENSION ON DT_REASON_DIMENSION.REASON_KEY = CSR_PATRON_ORDER_DETAIL.REASON_KEY
WHERE
    (
     (
      CASE  
WHEN ((CSR_PATRON_ORDER_DETAIL.AUTOLOAD_ENROLL_ACTION IS NOT NULL)) 
    THEN CSR_PATRON_ORDER_DETAIL.LINE_ITEM_TYPE || ' (' || CSR_PATRON_ORDER_DETAIL.AUTOLOAD_ENROLL_ACTION || ')' 
ELSE CSR_PATRON_ORDER_DETAIL.LINE_ITEM_TYPE
END  LIKE  '%Adjustment%'
      OR
      CASE  
WHEN ((CSR_PATRON_ORDER_DETAIL.AUTOLOAD_ENROLL_ACTION IS NOT NULL)) 
    THEN CSR_PATRON_ORDER_DETAIL.LINE_ITEM_TYPE || ' (' || CSR_PATRON_ORDER_DETAIL.AUTOLOAD_ENROLL_ACTION || ')' 
ELSE CSR_PATRON_ORDER_DETAIL.LINE_ITEM_TYPE
END  IN  ( 'Pass Removal','Pass Conversion','Transit Account Trip Correction','Transit Account Trip Void','Transit Account Tap Void'  )
     )
     OR
     (
      CASE  
WHEN ((CSR_PATRON_ORDER_DETAIL.AUTOLOAD_ENROLL_ACTION IS NOT NULL)) 
    THEN CSR_PATRON_ORDER_DETAIL.LINE_ITEM_TYPE || ' (' || CSR_PATRON_ORDER_DETAIL.AUTOLOAD_ENROLL_ACTION || ')' 
ELSE CSR_PATRON_ORDER_DETAIL.LINE_ITEM_TYPE
END  LIKE  '%Refund%'
      AND
      CASE  
WHEN ((CSR_PATRON_ORDER_DETAIL.AUTOLOAD_ENROLL_ACTION IS NOT NULL)) 
    THEN CSR_PATRON_ORDER_DETAIL.LINE_ITEM_TYPE || ' (' || CSR_PATRON_ORDER_DETAIL.AUTOLOAD_ENROLL_ACTION || ')' 
ELSE CSR_PATRON_ORDER_DETAIL.LINE_ITEM_TYPE
END  <>  'Refund Issue Media'
     )
    );
"""
