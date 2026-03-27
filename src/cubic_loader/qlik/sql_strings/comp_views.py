COMP_B_TXN_A = """
    DROP VIEW IF EXISTS ods.farerev_payg_trip_txn_a;
    CREATE VIEW ods.farerev_payg_trip_txn_a AS
    WITH EDW_FAREREV_PAYG_TRIP_TXN AS
    (
    SELECT
        m.txn_channel_display,
        CAST('B' AS VARCHAR) AS computation_type,
        TO_TIMESTAMP(CAST(ut.operating_day_key AS VARCHAR), 'YYYYMMDD') AS operating_date,
        ut.transaction_dtm,
        TO_TIMESTAMP(CAST(ut.posting_day_key AS VARCHAR), 'YYYYMMDD') AS posting_date,
        TO_TIMESTAMP(CAST(ut.settlement_day_key AS VARCHAR), 'YYYYMMDD') AS settlement_date,
        th.transit_mode_name,
        ut.device_id,
        ut.dw_transaction_id,
        ut.transit_account_id,
        ut.patron_trip_id,
        NULL AS tap_id,
        tr.fare_rule_description,
        ut.transfer_sequence_nbr,
        pt.payment_type_name,
        cd.bin,
        -(sum(((tp.bankcard_value + CASE WHEN (((tp.bankcard_payment_id IS NOT NULL) OR (mt.payg_flag = 1))) THEN (tp.uncollectible_amount) ELSE 0 END) + CASE WHEN (((s.purse_load_id IS NOT NULL) AND (mt.payg_flag = 1))) THEN (tp.stored_value) ELSE 0 END))) AS fare_revenue,
        CASE
            WHEN (((ut.transfer_sequence_nbr > 0)
            AND (ut.fare_due != 0))) THEN ('Transfer')
            ELSE NULL
        END AS extension_charge_reason,
        bcp.retrieval_ref_nbr,
        pt.payment_type_key,
        ut.operating_day_key,
        ut.settlement_day_key,
        ut.posting_day_key
    FROM
        ods.edw_use_transaction AS ut
    INNER JOIN ods.edw_patron_trip AS tr ON
        (((tr.patron_trip_id = ut.patron_trip_id)
            AND (tr."source" = ut."source")))
    INNER JOIN ods.edw_trip_payment AS tp ON
        (((tp.patron_trip_id = tr.patron_trip_id)
            AND (tp."source" = tr."source")
                AND (tp.trip_price_count = ut.trip_price_count)
                    AND (((tp.je_is_fare_adjustment = 0)
                        AND (ut.ride_type_key != 24)
                            AND (tp.is_reversal = 1)
                                AND (ut.fare_due < 0))
                        OR ((tp.je_is_fare_adjustment = 0)
                            AND (ut.ride_type_key != 24)
                                AND (tp.is_reversal = 0)
                                    AND (ut.fare_due > 0))
                            OR ((tp.je_is_fare_adjustment = 1)
                                AND (ut.ride_type_key = 24)))))
    LEFT JOIN ods.edw_sale_transaction AS s ON
        (((s.purse_load_id = tp.purse_load_id)
            AND (s.sale_type_key = 26)))
    LEFT JOIN ods.edw_sale_transaction AS bcp ON
        (((bcp.bankcard_payment_id = tp.bankcard_payment_id)
            AND (bcp.sale_type_key = 20)
                AND (bcp.bankcard_payment_type_key IN (1, 2, 3))))
    INNER JOIN (
        SELECT
            payment_type_name,
            payment_type_key
        FROM
            ods.edw_payment_type_dimension
        WHERE
            (payment_type_key = 2)) AS pt ON
        ((1 = 1))
    LEFT JOIN ods.edw_card_dimension AS cd ON
        ((cd.card_key = ut.card_key))
    INNER JOIN ods.edw_media_type_dimension AS mt ON
        ((mt.media_type_key = ut.media_type_key))
    INNER JOIN (
        SELECT
            txn_channel_display
        FROM
            ods.edw_txn_channel_map
        WHERE
            ((txn_source = 'UseTxn')
                AND (sales_channel_key = 8)
                    AND (payment_type_key = 2))) AS m ON
        ((1 = 1))
    LEFT JOIN ods.edw_transaction_history AS th ON
        ((th.dw_transaction_id = ut.dw_transaction_id))
    WHERE
        ((ut.bankcard_payment_value != 0)
            OR (ut.uncollectible_amount != 0)
                OR (ut.value_changed != 0))
    GROUP BY
        m.txn_channel_display,
        ut.transaction_dtm,
        th.transit_mode_name,
        ut.device_id,
        ut.dw_transaction_id,
        ut.transit_account_id,
        ut.patron_trip_id,
        tr.fare_rule_description,
        ut.transfer_sequence_nbr,
        pt.payment_type_name,
        cd.bin,
        CASE
            WHEN (((ut.transfer_sequence_nbr > 0)
                AND (ut.fare_due != 0))) THEN ('Transfer')
            ELSE NULL
        END,
        bcp.retrieval_ref_nbr,
        pt.payment_type_key,
        ut.operating_day_key,
        ut.settlement_day_key,
        ut.posting_day_key
    HAVING
        (sum(((tp.bankcard_value + CASE WHEN (((tp.bankcard_payment_id IS NOT NULL) OR (mt.payg_flag = 1))) THEN (tp.uncollectible_amount) ELSE 0 END) + CASE WHEN (((s.purse_load_id IS NOT NULL) AND (mt.payg_flag = 1))) THEN (tp.stored_value) ELSE 0 END)) != 0)
    UNION ALL (
    SELECT
    m.txn_channel_display,
    CAST('B' AS VARCHAR) AS computation_type,
    TO_TIMESTAMP(CAST(s.operating_day_key AS VARCHAR), 'YYYYMMDD') AS operating_date,
    s.transaction_dtm,
    TO_TIMESTAMP(CAST(s.posting_day_key AS VARCHAR), 'YYYYMMDD') AS posting_date,
    TO_TIMESTAMP(CAST(s.settlement_day_key AS VARCHAR), 'YYYYMMDD') AS settlement_date,
    th.transit_mode_name,
    s.device_id,
    s.dw_transaction_id,
    s.transit_account_id,
    NULL AS patron_trip_id,
    s.tap_id,
    NULL AS fare_rule_description,
    NULL AS transfer_sequence_nbr,
    pt.payment_type_name,
    cd.bin,
    sp.payment_value AS fare_revenue,
    NULL AS extension_charge_reason,
    sp.retrieval_ref_nbr,
    pt.payment_type_key,
    s.operating_day_key,
    s.settlement_day_key,
    s.posting_day_key
    FROM
    ods.edw_sale_transaction AS s
    INNER JOIN ods.edw_sale_txn_payment AS sp ON
    (((sp.dw_transaction_id = s.dw_transaction_id)
        AND (sp.transaction_dtm = s.transaction_dtm)))
    INNER JOIN ods.edw_media_type_dimension AS mt ON
    ((mt.media_type_key = s.media_type_key))
    INNER JOIN ods.edw_txn_channel_map AS m ON
    (((m.txn_source = 'UseTxn')
        AND (m.sales_channel_key = 14)
            AND (m.payment_type_key = CASE
                WHEN ((s.reason_key IN (990951, 990961))) THEN (2)
                ELSE 4
            END)))
    INNER JOIN ods.edw_payment_type_dimension AS pt ON
    ((pt.payment_type_key = m.payment_type_key))
    LEFT JOIN ods.edw_read_transaction AS rt ON
    ((rt.tap_id = s.tap_id))
    LEFT JOIN ods.edw_transaction_history AS th ON
    ((th.dw_transaction_id = rt.dw_transaction_id))
    LEFT JOIN ods.edw_card_dimension AS cd ON
    ((cd.card_key = rt.card_key))
    WHERE
    ((s.operating_day_key IS NOT NULL)
        AND (s.sale_type_key = 22)
            AND (s.reason_key IN (990951, 990961, 990952, 990962))
                AND (mt.payg_flag = 1)))),
    EDW_FARE_REVENUE_REPORT_SCHEDULE_A AS(
    SELECT
        DISTINCT s.due_day_key,
        s.adden_max_operating_day_key,
        s.adden_min_settlement_day_key,
        s.adden_max_settlement_day_key,
        s.deposit_due_day_key
    FROM
        ods.edw_fare_revenue_report_schedule AS s),
    EDW_farerev_payg_trip_txn_a AS(
    SELECT
        rs.due_day_key AS report_due_day_key,
        CAST('Y' AS VARCHAR) AS addendum,
        t.*
    FROM
        EDW_FARE_REVENUE_REPORT_SCHEDULE_A AS rs
    INNER JOIN EDW_FAREREV_PAYG_TRIP_TXN AS t ON
        (((t.operating_day_key <= rs.adden_max_operating_day_key)
            AND (t.settlement_day_key BETWEEN rs.adden_min_settlement_day_key AND rs.adden_max_settlement_day_key)))),
    farerev_payg_trip_txn_a AS (
    SELECT
        report_due_day_key,
        addendum,
        txn_channel_display,
        computation_type,
        operating_date,
        transaction_dtm,
        posting_date,
        settlement_date,
        transit_mode_name,
        device_id,
        dw_transaction_id,
        transit_account_id,
        patron_trip_id,
        tap_id,
        fare_rule_description,
        transfer_sequence_nbr,
        payment_type_name,
        bin,
        round((fare_revenue / 100), 2) AS fare_revenue,
        extension_charge_reason,
        retrieval_ref_nbr,
        operating_day_key,
        settlement_day_key,
        posting_day_key
    FROM
        EDW_farerev_payg_trip_txn_a)
    SELECT *
    FROM farerev_payg_trip_txn_a;
"""

COMP_B_TXN_C = """
    DROP VIEW IF EXISTS ods.farerev_payg_trip_txn_c;
    CREATE VIEW ods.farerev_payg_trip_txn_c AS
    WITH EDW_FAREREV_PAYG_TRIP_TXN AS
    (
    SELECT
        m.txn_channel_display,
        CAST('B' AS VARCHAR) AS computation_type,
        TO_TIMESTAMP(CAST(ut.operating_day_key AS VARCHAR), 'YYYYMMDD') AS operating_date,
        ut.transaction_dtm,
        TO_TIMESTAMP(CAST(ut.posting_day_key AS VARCHAR), 'YYYYMMDD') AS posting_date,
        TO_TIMESTAMP(CAST(ut.settlement_day_key AS VARCHAR), 'YYYYMMDD') AS settlement_date,
        th.transit_mode_name,
        ut.device_id,
        ut.dw_transaction_id,
        ut.transit_account_id,
        ut.patron_trip_id,
        NULL AS tap_id,
        tr.fare_rule_description,
        ut.transfer_sequence_nbr,
        pt.payment_type_name,
        cd.bin,
        -(sum(((tp.bankcard_value + CASE WHEN (((tp.bankcard_payment_id IS NOT NULL) OR (mt.payg_flag = 1))) THEN (tp.uncollectible_amount) ELSE 0 END) + CASE WHEN (((s.purse_load_id IS NOT NULL) AND (mt.payg_flag = 1))) THEN (tp.stored_value) ELSE 0 END))) AS fare_revenue,
        CASE
            WHEN (((ut.transfer_sequence_nbr > 0)
            AND (ut.fare_due != 0))) THEN ('Transfer')
            ELSE NULL
        END AS extension_charge_reason,
        bcp.retrieval_ref_nbr,
        pt.payment_type_key,
        ut.operating_day_key,
        ut.settlement_day_key,
        ut.posting_day_key
    FROM
        ods.edw_use_transaction AS ut
    INNER JOIN ods.edw_patron_trip AS tr ON
        (((tr.patron_trip_id = ut.patron_trip_id)
            AND (tr."source" = ut."source")))
    INNER JOIN ods.edw_trip_payment AS tp ON
        (((tp.patron_trip_id = tr.patron_trip_id)
            AND (tp."source" = tr."source")
                AND (tp.trip_price_count = ut.trip_price_count)
                    AND (((tp.je_is_fare_adjustment = 0)
                        AND (ut.ride_type_key != 24)
                            AND (tp.is_reversal = 1)
                                AND (ut.fare_due < 0))
                        OR ((tp.je_is_fare_adjustment = 0)
                            AND (ut.ride_type_key != 24)
                                AND (tp.is_reversal = 0)
                                    AND (ut.fare_due > 0))
                            OR ((tp.je_is_fare_adjustment = 1)
                                AND (ut.ride_type_key = 24)))))
    LEFT JOIN ods.edw_sale_transaction AS s ON
        (((s.purse_load_id = tp.purse_load_id)
            AND (s.sale_type_key = 26)))
    LEFT JOIN ods.edw_sale_transaction AS bcp ON
        (((bcp.bankcard_payment_id = tp.bankcard_payment_id)
            AND (bcp.sale_type_key = 20)
                AND (bcp.bankcard_payment_type_key IN (1, 2, 3))))
    INNER JOIN (
        SELECT
            payment_type_name,
            payment_type_key
        FROM
            ods.edw_payment_type_dimension
        WHERE
            (payment_type_key = 2)) AS pt ON
        ((1 = 1))
    LEFT JOIN ods.edw_card_dimension AS cd ON
        ((cd.card_key = ut.card_key))
    INNER JOIN ods.edw_media_type_dimension AS mt ON
        ((mt.media_type_key = ut.media_type_key))
    INNER JOIN (
        SELECT
            txn_channel_display
        FROM
            ods.edw_txn_channel_map
        WHERE
            ((txn_source = 'UseTxn')
                AND (sales_channel_key = 8)
                    AND (payment_type_key = 2))) AS m ON
        ((1 = 1))
    LEFT JOIN ods.edw_transaction_history AS th ON
        ((th.dw_transaction_id = ut.dw_transaction_id))
    WHERE
        ((ut.bankcard_payment_value != 0)
            OR (ut.uncollectible_amount != 0)
                OR (ut.value_changed != 0))
    GROUP BY
        m.txn_channel_display,
        ut.transaction_dtm,
        th.transit_mode_name,
        ut.device_id,
        ut.dw_transaction_id,
        ut.transit_account_id,
        ut.patron_trip_id,
        tr.fare_rule_description,
        ut.transfer_sequence_nbr,
        pt.payment_type_name,
        cd.bin,
        CASE
            WHEN (((ut.transfer_sequence_nbr > 0)
                AND (ut.fare_due != 0))) THEN ('Transfer')
            ELSE NULL
        END,
        bcp.retrieval_ref_nbr,
        pt.payment_type_key,
        ut.operating_day_key,
        ut.settlement_day_key,
        ut.posting_day_key
    HAVING
        (sum(((tp.bankcard_value + CASE WHEN (((tp.bankcard_payment_id IS NOT NULL) OR (mt.payg_flag = 1))) THEN (tp.uncollectible_amount) ELSE 0 END) + CASE WHEN (((s.purse_load_id IS NOT NULL) AND (mt.payg_flag = 1))) THEN (tp.stored_value) ELSE 0 END)) != 0)
    UNION ALL (
    SELECT
    m.txn_channel_display,
    CAST('B' AS VARCHAR) AS computation_type,
    TO_TIMESTAMP(CAST(s.operating_day_key AS VARCHAR), 'YYYYMMDD') AS operating_date,
    s.transaction_dtm,
    TO_TIMESTAMP(CAST(s.posting_day_key AS VARCHAR), 'YYYYMMDD') AS posting_date,
    TO_TIMESTAMP(CAST(s.settlement_day_key AS VARCHAR), 'YYYYMMDD') AS settlement_date,
    th.transit_mode_name,
    s.device_id,
    s.dw_transaction_id,
    s.transit_account_id,
    NULL AS patron_trip_id,
    s.tap_id,
    NULL AS fare_rule_description,
    NULL AS transfer_sequence_nbr,
    pt.payment_type_name,
    cd.bin,
    sp.payment_value AS fare_revenue,
    NULL AS extension_charge_reason,
    sp.retrieval_ref_nbr,
    pt.payment_type_key,
    s.operating_day_key,
    s.settlement_day_key,
    s.posting_day_key
    FROM
    ods.edw_sale_transaction AS s
    INNER JOIN ods.edw_sale_txn_payment AS sp ON
    (((sp.dw_transaction_id = s.dw_transaction_id)
        AND (sp.transaction_dtm = s.transaction_dtm)))
    INNER JOIN ods.edw_media_type_dimension AS mt ON
    ((mt.media_type_key = s.media_type_key))
    INNER JOIN ods.edw_txn_channel_map AS m ON
    (((m.txn_source = 'UseTxn')
        AND (m.sales_channel_key = 14)
            AND (m.payment_type_key = CASE
                WHEN ((s.reason_key IN (990951, 990961))) THEN (2)
                ELSE 4
            END)))
    INNER JOIN ods.edw_payment_type_dimension AS pt ON
    ((pt.payment_type_key = m.payment_type_key))
    LEFT JOIN ods.edw_read_transaction AS rt ON
    ((rt.tap_id = s.tap_id))
    LEFT JOIN ods.edw_transaction_history AS th ON
    ((th.dw_transaction_id = rt.dw_transaction_id))
    LEFT JOIN ods.edw_card_dimension AS cd ON
    ((cd.card_key = rt.card_key))
    WHERE
    ((s.operating_day_key IS NOT NULL)
        AND (s.sale_type_key = 22)
            AND (s.reason_key IN (990951, 990961, 990952, 990962))
                AND (mt.payg_flag = 1)))),
    EDW_FARE_REVENUE_REPORT_SCHEDULE_A AS(
    SELECT
        DISTINCT s.due_day_key,
        s.adden_max_operating_day_key,
        s.adden_min_settlement_day_key,
        s.adden_max_settlement_day_key,
        s.deposit_due_day_key
    FROM
        ods.edw_fare_revenue_report_schedule AS s),
    EDW_farerev_payg_trip_txn_a AS(
    SELECT
        rs.due_day_key AS report_due_day_key,
        CAST('Y' AS VARCHAR) AS addendum,
        t.*
    FROM
        EDW_FARE_REVENUE_REPORT_SCHEDULE_A AS rs
    INNER JOIN EDW_FAREREV_PAYG_TRIP_TXN AS t ON
        (((t.operating_day_key <= rs.adden_max_operating_day_key)
            AND (t.settlement_day_key BETWEEN rs.adden_min_settlement_day_key AND rs.adden_max_settlement_day_key)))),
    EDW_farerev_payg_trip_txn_c AS(
    SELECT
        rs.due_day_key AS report_due_day_key,
        CAST('N' AS VARCHAR) AS addendum,
        t.*
    FROM
        ods.edw_fare_revenue_report_schedule AS rs
    INNER JOIN EDW_FAREREV_PAYG_TRIP_TXN AS t ON
        (((t.operating_day_key = rs.comp_operating_day_key)
            AND (t.settlement_day_key <= rs.comp_max_settlement_day_key)))),
    farerev_payg_trip_txn_c AS(
    SELECT
        report_due_day_key,
        addendum,
        txn_channel_display,
        computation_type,
        operating_date,
        transaction_dtm,
        posting_date,
        settlement_date,
        transit_mode_name,
        device_id,
        dw_transaction_id,
        transit_account_id,
        patron_trip_id,
        tap_id,
        fare_rule_description,
        transfer_sequence_nbr,
        payment_type_name,
        bin,
        round((fare_revenue / 100), 2) AS fare_revenue,
        extension_charge_reason,
        retrieval_ref_nbr,
        operating_day_key,
        settlement_day_key,
        posting_day_key
    FROM
        EDW_farerev_payg_trip_txn_c)
    SELECT *
    FROM farerev_payg_trip_txn_c;
"""

COMP_A_TXN_A = """
    DROP VIEW IF EXISTS ods.farerev_prod_sales_txn_a;
    CREATE VIEW ods.farerev_prod_sales_txn_a
    AS
    with EDW_FAREREV_PROD_SALES_TXN AS (
    SELECT
        m.txn_channel_display,
        m.sales_channel_display,
        CAST('A' AS VARCHAR) AS computation_type,
        od.dtm AS operating_date,
        o.order_dtm AS transaction_dtm,
        CAST(li.source_inserted_dtm AS DATE) AS posting_date,
        sd.dtm AS settlement_date,
        dd.facility_name,
        dd.device_id,
        p.dw_transaction_id AS transaction_id,
        CAST(NULL AS VARCHAR) AS group_account_id,
        li.subsystem_account_ref AS transit_account_id,
        o.order_nbr,
        1 AS quantity,
        li.line_item_sequence AS order_line_item_sequence,
        pt.payment_type_name,
        CASE
            WHEN ((p.payment_type_key = 1)) THEN (p.payment_amount)
            ELSE 0
        END AS cash_received,
        CASE
            WHEN (((p.payment_type_key = 1)
            AND (li.line_item_type = 'Cash Overpayment'))) THEN (-((SELECT COALESCE(payment_amount, 0) FROM ods.edw_patron_order_payment WHERE ((dw_patron_order_line_item_id = p.dw_patron_order_line_item_id) AND (retrieval_ref_nbr = 'cash returned')))))
            ELSE 0
        END AS cash_returned,
        CASE
            WHEN ((p.payment_type_key = 1)) THEN (CASE
                WHEN ((li.line_item_type = 'Cash Overpayment')) THEN ((p.payment_amount + (
                SELECT
                    COALESCE(payment_amount, 0)
                FROM
                    ods.edw_patron_order_payment
                WHERE
                    ((dw_patron_order_line_item_id = p.dw_patron_order_line_item_id)
                        AND (retrieval_ref_nbr = 'cash returned')))))
                ELSE p.payment_amount
            END)
            ELSE 0
        END AS net_cash,
        COALESCE(li.item_total_discount_amount, 0) AS discount_amount,
        ((((COALESCE(p.transit_value, 0) + COALESCE(p.benefit_value, 0)) + COALESCE(p.refundable_purse_value, 0)) + COALESCE(p.pass_cost, 0)) + COALESCE(p.enablement_fee, 0)) AS fare_revenue,
        CASE
            WHEN (((fp.fare_prod_name IS NULL)
                AND ((p.transit_value != 0)
                    OR (p.benefit_value != 0)
                        OR (p.refundable_purse_value != 0)))) THEN ('Stored Value')
            ELSE fp.fare_prod_name
        END AS fare_product,
        COALESCE(p.transit_value, 0) AS transit_value,
        COALESCE(p.benefit_value, 0) AS benefit_value,
        COALESCE(p.refundable_purse_value, 0) AS refundable_purse_value,
        ((COALESCE(p.transit_value, 0) + COALESCE(p.benefit_value, 0)) + COALESCE(p.refundable_purse_value, 0)) AS total_stored_value,
        COALESCE(p.pass_cost, 0) AS pass_cost,
        COALESCE(p.enablement_fee, 0) AS enablement_fee,
        COALESCE(p.replacement_fee, 0) AS replacement_fee,
        COALESCE(p.product_value, 0) AS product_value,
        (COALESCE(p.payment_amount, 0) - (((((((((COALESCE(p.pass_cost, 0) + COALESCE(p.transit_value, 0)) + COALESCE(p.benefit_value, 0)) + COALESCE(p.refundable_purse_value, 0)) + COALESCE(p.enablement_fee, 0)) + COALESCE(p.replacement_fee, 0)) + COALESCE(p.shipping_fee, 0)) + COALESCE(p.card_fee, 0)) + COALESCE(p.deposit_value, 0)) + COALESCE(p.administrative_fee, 0))) AS overpayment,
        0 AS retail_commission_amount,
        p.retrieval_ref_nbr,
        li.line_item_status,
        li.line_item_type,
        rd.reason_name,
        pt.payment_type_key,
        li.operating_day_key,
        p.settlement_day_key
    FROM
        ods.edw_patron_order_payment AS p
    INNER JOIN ods.edw_patron_order_line_item AS li ON
        ((li.dw_patron_order_line_item_id = p.dw_patron_order_line_item_id))
    INNER JOIN ods.edw_patron_order AS o ON
        (((o.dw_patron_order_id = li.dw_patron_order_id)
            AND (lower(o.order_type) !~~ '%refund%')))
    INNER JOIN ods.edw_txn_channel_map AS m ON
        (((m.txn_group = 'Product Sales')
            AND (m.txn_source IN ('SalesOrder', 'RefundOrder'))
                AND (m.sales_channel_key = o.sales_channel_key)
                    AND (m.payment_type_key = p.payment_type_key)))
    INNER JOIN ods.edw_date_dimension AS od ON
        ((od.date_key = li.operating_day_key))
    INNER JOIN ods.edw_date_dimension AS sd ON
        ((sd.date_key = p.settlement_day_key))
    LEFT JOIN ods.edw_device_dimension AS dd ON
        ((dd.device_key = o.device_key))
    INNER JOIN ods.edw_payment_type_dimension AS pt ON
        ((pt.payment_type_key = p.payment_type_key))
    LEFT JOIN ods.edw_fare_product_dimension AS fp ON
        ((fp.fare_prod_key = li.fare_prod_key))
    LEFT JOIN ods.edw_reason_dimension AS rd ON
        ((rd.reason_key = li.reason_key))
    WHERE
        ((COALESCE(p.retrieval_ref_nbr, '.') != 'cash returned')
            AND ((p.transaction_category != -9)
                OR (o.order_type IN ('Sale', 'Refund'))))
    UNION ALL (
    SELECT
    m.txn_channel_display,
    m.sales_channel_display,
    CAST('A' AS VARCHAR) AS computation_type,
    od.dtm AS operating_date,
    st.transaction_dtm,
    pd.dtm AS posting_date,
    sd.dtm AS settlement_date,
    dd.facility_name,
    dd.device_id,
    st.dw_transaction_id AS transaction_id,
    cd.fin_customer_id AS group_account_id,
    CAST(st.transit_account_id AS VARCHAR) AS transit_account_id,
    po.order_nbr,
    1 AS quantity,
    li.line_item_sequence AS order_line_item_sequence,
    pt.payment_type_name,
    CASE
        WHEN ((sp.payment_type_key = 1)) THEN (sp.payment_value)
        ELSE 0
    END AS cash_received,
    0 AS cash_returned,
    CASE
        WHEN ((sp.payment_type_key = 1)) THEN (sp.payment_value)
        ELSE 0
    END AS net_cash,
    COALESCE(st.discount_amount, 0) AS discount_amount,
    ((((COALESCE(sp.value_changed, 0) + COALESCE(sp.benefit_value, 0)) + COALESCE(sp.refundable_purse_value, 0)) + COALESCE(sp.pass_cost, 0)) + COALESCE(sp.enablement_fee, 0)) AS fare_revenue,
    CASE
        WHEN (((fp.fare_prod_name IS NULL)
            AND ((sp.value_changed != 0)
                OR (sp.benefit_value != 0)
                    OR (sp.refundable_purse_value != 0)))) THEN ('Stored Value')
        ELSE fp.fare_prod_name
    END AS fare_product,
    COALESCE(sp.value_changed, 0) AS transit_value,
    COALESCE(sp.benefit_value, 0) AS benefit_value,
    COALESCE(sp.refundable_purse_value, 0) AS refundable_purse_value,
    ((COALESCE(sp.value_changed, 0) + COALESCE(sp.benefit_value, 0)) + COALESCE(sp.refundable_purse_value, 0)) AS total_stored_value,
    COALESCE(sp.pass_cost, 0) AS pass_cost,
    COALESCE(sp.enablement_fee, 0) AS enablement_fee,
    COALESCE(sp.replacement_fee, 0) AS replacement_fee,
    COALESCE(sp.net_value, 0) AS product_value,
    (COALESCE(sp.payment_value, 0) - (((((((((COALESCE(sp.pass_cost, 0) + COALESCE(sp.value_changed, 0)) + COALESCE(sp.benefit_value, 0)) + COALESCE(sp.refundable_purse_value, 0)) + COALESCE(sp.enablement_fee, 0)) + COALESCE(sp.replacement_fee, 0)) + COALESCE(sp.shipping_fee, 0)) + COALESCE(sp.card_fee, 0)) + COALESCE(sp.deposit_value, 0)) + COALESCE(sp.administrative_fee, 0))) AS overpayment,
    (COALESCE(st.product_commission_amount, 0) + COALESCE(st.fee_commission_amount, 0)) AS retail_commission_amount,
    sp.retrieval_ref_nbr,
    li.line_item_status,
    li.line_item_type,
    rd.reason_name,
    pt.payment_type_key,
    st.operating_day_key,
    st.settlement_day_key
    FROM
    ods.edw_sale_transaction AS st
    INNER JOIN ods.edw_sale_txn_payment AS sp ON
    (((sp.dw_transaction_id = st.dw_transaction_id)
        AND (sp.transaction_dtm = st.transaction_dtm)))
    LEFT JOIN ods.edw_patron_order_line_item AS li ON
    ((li.dw_patron_order_line_item_id = st.dw_patron_order_line_item_id))
    LEFT JOIN ods.edw_patron_order AS po ON
    ((po.dw_patron_order_id = li.dw_patron_order_id))
    INNER JOIN ods.edw_txn_channel_map AS m ON
    (((m.txn_group = 'Product Sales')
        AND (m.sales_channel_key = st.sales_channel_key)
            AND (m.payment_type_key = sp.payment_type_key)
                AND ((m.txn_source IN ('SalesTxn', 'RefundTxn'))
                    OR ((m.txn_source = 'SalesOrder')
                        AND (li.line_item_type = 'Refund Cash Overpayment')))))
    INNER JOIN ods.edw_date_dimension AS od ON
    ((od.date_key = st.operating_day_key))
    INNER JOIN ods.edw_date_dimension AS sd ON
    ((sd.date_key = st.settlement_day_key))
    INNER JOIN ods.edw_date_dimension AS pd ON
    ((pd.date_key = st.posting_day_key))
    LEFT JOIN ods.edw_device_dimension AS dd ON
    ((dd.device_key = st.device_key))
    INNER JOIN ods.edw_payment_type_dimension AS pt ON
    ((pt.payment_type_key = sp.payment_type_key))
    LEFT JOIN ods.edw_fare_product_dimension AS fp ON
    ((fp.fare_prod_key = st.fare_prod_key))
    LEFT JOIN ods.edw_reason_dimension AS rd ON
    ((rd.reason_key = st.reason_key))
    LEFT JOIN ods.edw_customer_dimension AS cd ON
    ((cd.customer_key = st.customer_key))
    WHERE
    (st.sale_type_key IN (21, 30, 31, 23)))),
    EDW_FARE_REVENUE_REPORT_SCHEDULE_A AS(
    SELECT
        DISTINCT s.due_day_key,
        s.adden_max_operating_day_key,
        s.adden_min_settlement_day_key,
        s.adden_max_settlement_day_key,
        s.deposit_due_day_key
    FROM
        ods.edw_fare_revenue_report_schedule AS s),
    EDW_farerev_prod_sales_txn_a AS (
    SELECT
        rs.due_day_key AS report_due_day_key,
        CAST('Y' AS VARCHAR) AS addendum,
        t.*
    FROM
        EDW_FARE_REVENUE_REPORT_SCHEDULE_A AS rs
    INNER JOIN EDW_FAREREV_PROD_SALES_TXN AS t ON
        (((t.operating_day_key <= rs.adden_max_operating_day_key)
            AND (t.settlement_day_key BETWEEN rs.adden_min_settlement_day_key AND rs.adden_max_settlement_day_key)))),
    farerev_prod_sales_txn_a AS (
    SELECT DISTINCT
        report_due_day_key,
        addendum,
        txn_channel_display,
        sales_channel_display,
        computation_type,
        operating_date,
        transaction_dtm,
        posting_date,
        settlement_date,
        facility_name,
        device_id,
        transaction_id,
        group_account_id,
        transit_account_id,
        order_nbr,
        quantity,
        order_line_item_sequence,
        payment_type_name,
        round((cash_received / 100), 2) AS cash_received,
        round((cash_returned / 100), 2) AS cash_returned,
        round((net_cash / 100), 2) AS net_cash,
        round((discount_amount / 100), 2) AS discount_amount,
        round((fare_revenue / 100), 2) AS fare_revenue,
        fare_product,
        round((transit_value / 100), 2) AS transit_value,
        round((benefit_value / 100), 2) AS benefit_value,
        round((refundable_purse_value / 100), 2) AS refundable_purse_value,
        round((total_stored_value / 100), 2) AS total_stored_value,
        round((pass_cost / 100), 2) AS pass_cost,
        round((enablement_fee / 100), 2) AS enablement_fee,
        round((replacement_fee / 100), 2) AS replacement_fee,
        round((product_value / 100), 2) AS product_value,
        round((overpayment / 100), 2) AS overpayment,
        round((CAST(retail_commission_amount AS NUMERIC) / 100), 2) AS retail_commission_amount,
        retrieval_ref_nbr,
        line_item_status,
        line_item_type,
        reason_name,
        operating_day_key,
        settlement_day_key
    FROM
        EDW_farerev_prod_sales_txn_a)
    SELECT * FROM farerev_prod_sales_txn_a;
"""

COMP_A_TXN_C = """
    DROP VIEW IF EXISTS ods.farerev_prod_sales_txn_c;
    CREATE VIEW ods.farerev_prod_sales_txn_c
    AS
    with EDW_FAREREV_PROD_SALES_TXN AS (
    SELECT
        m.txn_channel_display,
        m.sales_channel_display,
        CAST('A' AS VARCHAR) AS computation_type,
        od.dtm AS operating_date,
        o.order_dtm AS transaction_dtm,
        CAST(li.source_inserted_dtm AS DATE) AS posting_date,
        sd.dtm AS settlement_date,
        dd.facility_name,
        dd.device_id,
        p.dw_transaction_id AS transaction_id,
        CAST(NULL AS VARCHAR) AS group_account_id,
        li.subsystem_account_ref AS transit_account_id,
        o.order_nbr,
        1 AS quantity,
        li.line_item_sequence AS order_line_item_sequence,
        pt.payment_type_name,
        CASE
            WHEN ((p.payment_type_key = 1)) THEN (p.payment_amount)
            ELSE 0
        END AS cash_received,
        CASE
            WHEN (((p.payment_type_key = 1)
            AND (li.line_item_type = 'Cash Overpayment'))) THEN (-((SELECT COALESCE(payment_amount, 0) FROM ods.edw_patron_order_payment WHERE ((dw_patron_order_line_item_id = p.dw_patron_order_line_item_id) AND (retrieval_ref_nbr = 'cash returned')))))
            ELSE 0
        END AS cash_returned,
        CASE
            WHEN ((p.payment_type_key = 1)) THEN (CASE
                WHEN ((li.line_item_type = 'Cash Overpayment')) THEN ((p.payment_amount + (
                SELECT
                    COALESCE(payment_amount, 0)
                FROM
                    ods.edw_patron_order_payment
                WHERE
                    ((dw_patron_order_line_item_id = p.dw_patron_order_line_item_id)
                        AND (retrieval_ref_nbr = 'cash returned')))))
                ELSE p.payment_amount
            END)
            ELSE 0
        END AS net_cash,
        COALESCE(li.item_total_discount_amount, 0) AS discount_amount,
        ((((COALESCE(p.transit_value, 0) + COALESCE(p.benefit_value, 0)) + COALESCE(p.refundable_purse_value, 0)) + COALESCE(p.pass_cost, 0)) + COALESCE(p.enablement_fee, 0)) AS fare_revenue,
        CASE
            WHEN (((fp.fare_prod_name IS NULL)
                AND ((p.transit_value != 0)
                    OR (p.benefit_value != 0)
                        OR (p.refundable_purse_value != 0)))) THEN ('Stored Value')
            ELSE fp.fare_prod_name
        END AS fare_product,
        COALESCE(p.transit_value, 0) AS transit_value,
        COALESCE(p.benefit_value, 0) AS benefit_value,
        COALESCE(p.refundable_purse_value, 0) AS refundable_purse_value,
        ((COALESCE(p.transit_value, 0) + COALESCE(p.benefit_value, 0)) + COALESCE(p.refundable_purse_value, 0)) AS total_stored_value,
        COALESCE(p.pass_cost, 0) AS pass_cost,
        COALESCE(p.enablement_fee, 0) AS enablement_fee,
        COALESCE(p.replacement_fee, 0) AS replacement_fee,
        COALESCE(p.product_value, 0) AS product_value,
        (COALESCE(p.payment_amount, 0) - (((((((((COALESCE(p.pass_cost, 0) + COALESCE(p.transit_value, 0)) + COALESCE(p.benefit_value, 0)) + COALESCE(p.refundable_purse_value, 0)) + COALESCE(p.enablement_fee, 0)) + COALESCE(p.replacement_fee, 0)) + COALESCE(p.shipping_fee, 0)) + COALESCE(p.card_fee, 0)) + COALESCE(p.deposit_value, 0)) + COALESCE(p.administrative_fee, 0))) AS overpayment,
        0 AS retail_commission_amount,
        p.retrieval_ref_nbr,
        li.line_item_status,
        li.line_item_type,
        rd.reason_name,
        pt.payment_type_key,
        li.operating_day_key,
        p.settlement_day_key
    FROM
        ods.edw_patron_order_payment AS p
    INNER JOIN ods.edw_patron_order_line_item AS li ON
        ((li.dw_patron_order_line_item_id = p.dw_patron_order_line_item_id))
    INNER JOIN ods.edw_patron_order AS o ON
        (((o.dw_patron_order_id = li.dw_patron_order_id)
            AND (lower(o.order_type) !~~ '%refund%')))
    INNER JOIN ods.edw_txn_channel_map AS m ON
        (((m.txn_group = 'Product Sales')
            AND (m.txn_source IN ('SalesOrder', 'RefundOrder'))
                AND (m.sales_channel_key = o.sales_channel_key)
                    AND (m.payment_type_key = p.payment_type_key)))
    INNER JOIN ods.edw_date_dimension AS od ON
        ((od.date_key = li.operating_day_key))
    INNER JOIN ods.edw_date_dimension AS sd ON
        ((sd.date_key = p.settlement_day_key))
    LEFT JOIN ods.edw_device_dimension AS dd ON
        ((dd.device_key = o.device_key))
    INNER JOIN ods.edw_payment_type_dimension AS pt ON
        ((pt.payment_type_key = p.payment_type_key))
    LEFT JOIN ods.edw_fare_product_dimension AS fp ON
        ((fp.fare_prod_key = li.fare_prod_key))
    LEFT JOIN ods.edw_reason_dimension AS rd ON
        ((rd.reason_key = li.reason_key))
    WHERE
        ((COALESCE(p.retrieval_ref_nbr, '.') != 'cash returned')
            AND ((p.transaction_category != -9)
                OR (o.order_type IN ('Sale', 'Refund'))))
    UNION ALL (
    SELECT
    m.txn_channel_display,
    m.sales_channel_display,
    CAST('A' AS VARCHAR) AS computation_type,
    od.dtm AS operating_date,
    st.transaction_dtm,
    pd.dtm AS posting_date,
    sd.dtm AS settlement_date,
    dd.facility_name,
    dd.device_id,
    st.dw_transaction_id AS transaction_id,
    cd.fin_customer_id AS group_account_id,
    CAST(st.transit_account_id AS VARCHAR) AS transit_account_id,
    po.order_nbr,
    1 AS quantity,
    li.line_item_sequence AS order_line_item_sequence,
    pt.payment_type_name,
    CASE
        WHEN ((sp.payment_type_key = 1)) THEN (sp.payment_value)
        ELSE 0
    END AS cash_received,
    0 AS cash_returned,
    CASE
        WHEN ((sp.payment_type_key = 1)) THEN (sp.payment_value)
        ELSE 0
    END AS net_cash,
    COALESCE(st.discount_amount, 0) AS discount_amount,
    ((((COALESCE(sp.value_changed, 0) + COALESCE(sp.benefit_value, 0)) + COALESCE(sp.refundable_purse_value, 0)) + COALESCE(sp.pass_cost, 0)) + COALESCE(sp.enablement_fee, 0)) AS fare_revenue,
    CASE
        WHEN (((fp.fare_prod_name IS NULL)
            AND ((sp.value_changed != 0)
                OR (sp.benefit_value != 0)
                    OR (sp.refundable_purse_value != 0)))) THEN ('Stored Value')
        ELSE fp.fare_prod_name
    END AS fare_product,
    COALESCE(sp.value_changed, 0) AS transit_value,
    COALESCE(sp.benefit_value, 0) AS benefit_value,
    COALESCE(sp.refundable_purse_value, 0) AS refundable_purse_value,
    ((COALESCE(sp.value_changed, 0) + COALESCE(sp.benefit_value, 0)) + COALESCE(sp.refundable_purse_value, 0)) AS total_stored_value,
    COALESCE(sp.pass_cost, 0) AS pass_cost,
    COALESCE(sp.enablement_fee, 0) AS enablement_fee,
    COALESCE(sp.replacement_fee, 0) AS replacement_fee,
    COALESCE(sp.net_value, 0) AS product_value,
    (COALESCE(sp.payment_value, 0) - (((((((((COALESCE(sp.pass_cost, 0) + COALESCE(sp.value_changed, 0)) + COALESCE(sp.benefit_value, 0)) + COALESCE(sp.refundable_purse_value, 0)) + COALESCE(sp.enablement_fee, 0)) + COALESCE(sp.replacement_fee, 0)) + COALESCE(sp.shipping_fee, 0)) + COALESCE(sp.card_fee, 0)) + COALESCE(sp.deposit_value, 0)) + COALESCE(sp.administrative_fee, 0))) AS overpayment,
    (COALESCE(st.product_commission_amount, 0) + COALESCE(st.fee_commission_amount, 0)) AS retail_commission_amount,
    sp.retrieval_ref_nbr,
    li.line_item_status,
    li.line_item_type,
    rd.reason_name,
    pt.payment_type_key,
    st.operating_day_key,
    st.settlement_day_key
    FROM
    ods.edw_sale_transaction AS st
    INNER JOIN ods.edw_sale_txn_payment AS sp ON
    (((sp.dw_transaction_id = st.dw_transaction_id)
        AND (sp.transaction_dtm = st.transaction_dtm)))
    LEFT JOIN ods.edw_patron_order_line_item AS li ON
    ((li.dw_patron_order_line_item_id = st.dw_patron_order_line_item_id))
    LEFT JOIN ods.edw_patron_order AS po ON
    ((po.dw_patron_order_id = li.dw_patron_order_id))
    INNER JOIN ods.edw_txn_channel_map AS m ON
    (((m.txn_group = 'Product Sales')
        AND (m.sales_channel_key = st.sales_channel_key)
            AND (m.payment_type_key = sp.payment_type_key)
                AND ((m.txn_source IN ('SalesTxn', 'RefundTxn'))
                    OR ((m.txn_source = 'SalesOrder')
                        AND (li.line_item_type = 'Refund Cash Overpayment')))))
    INNER JOIN ods.edw_date_dimension AS od ON
    ((od.date_key = st.operating_day_key))
    INNER JOIN ods.edw_date_dimension AS sd ON
    ((sd.date_key = st.settlement_day_key))
    INNER JOIN ods.edw_date_dimension AS pd ON
    ((pd.date_key = st.posting_day_key))
    LEFT JOIN ods.edw_device_dimension AS dd ON
    ((dd.device_key = st.device_key))
    INNER JOIN ods.edw_payment_type_dimension AS pt ON
    ((pt.payment_type_key = sp.payment_type_key))
    LEFT JOIN ods.edw_fare_product_dimension AS fp ON
    ((fp.fare_prod_key = st.fare_prod_key))
    LEFT JOIN ods.edw_reason_dimension AS rd ON
    ((rd.reason_key = st.reason_key))
    LEFT JOIN ods.edw_customer_dimension AS cd ON
    ((cd.customer_key = st.customer_key))
    WHERE
    (st.sale_type_key IN (21, 30, 31, 23)))),
    EDW_farerev_prod_sales_txn_c AS
    (SELECT
        rs.due_day_key AS report_due_day_key,
        CAST('N' AS VARCHAR) AS addendum,
        t.*
    FROM
        ods.edw_fare_revenue_report_schedule AS rs
    INNER JOIN EDW_FAREREV_PROD_SALES_TXN AS t ON
        (((t.operating_day_key = rs.comp_operating_day_key)
            AND (t.settlement_day_key <= rs.comp_max_settlement_day_key)))),
    farerev_prod_sales_txn_c AS
    (SELECT DISTINCT
        report_due_day_key,
        addendum,
        txn_channel_display,
        sales_channel_display,
        computation_type,
        operating_date,
        transaction_dtm,
        posting_date,
        settlement_date,
        facility_name,
        device_id,
        transaction_id,
        group_account_id,
        transit_account_id,
        order_nbr,
        quantity,
        order_line_item_sequence,
        payment_type_name,
        round((cash_received / 100), 2) AS cash_received,
        round((cash_returned / 100), 2) AS cash_returned,
        round((net_cash / 100), 2) AS net_cash,
        round((discount_amount / 100), 2) AS discount_amount,
        round((fare_revenue / 100), 2) AS fare_revenue,
        fare_product,
        round((transit_value / 100), 2) AS transit_value,
        round((benefit_value / 100), 2) AS benefit_value,
        round((refundable_purse_value / 100), 2) AS refundable_purse_value,
        round((total_stored_value / 100), 2) AS total_stored_value,
        round((pass_cost / 100), 2) AS pass_cost,
        round((enablement_fee / 100), 2) AS enablement_fee,
        round((replacement_fee / 100), 2) AS replacement_fee,
        round((product_value / 100), 2) AS product_value,
        round((overpayment / 100), 2) AS overpayment,
        round((CAST(retail_commission_amount AS NUMERIC) / 100), 2) AS retail_commission_amount,
        retrieval_ref_nbr,
        line_item_status,
        line_item_type,
        reason_name,
        operating_day_key,
        settlement_day_key
    FROM
        EDW_farerev_prod_sales_txn_c)
    SELECT * FROM farerev_prod_sales_txn_c;
"""
