import sqlalchemy as sa
from dmap_import.schemas import SqlBase


class SaleTransaction(SqlBase):
    """Table for A.3.3 SALE TRANSACTION"""

    # pylint: disable=too-few-public-methods
    # pylint: disable=R0801

    __tablename__ = "sale_transaction"

    pk_id = sa.Column(sa.Integer, primary_key=True)
    id = sa.Column(sa.String(), nullable=True)
    inserted_dtm = sa.Column(sa.DateTime, nullable=True)
    updated_dtm = sa.Column(sa.DateTime, nullable=True)
    depersonalized_dtm = sa.Column(sa.DateTime, nullable=True)
    updated_flag = sa.Column(sa.Integer, nullable=True)
    customer_email = sa.Column(sa.String(), nullable=True)
    customer_address = sa.Column(sa.String(), nullable=True)
    customer_city = sa.Column(sa.String(), nullable=True)
    customer_state = sa.Column(sa.String(), nullable=True)
    customer_postal_code = sa.Column(sa.Integer, nullable=True)
    language_id = sa.Column(sa.Integer, nullable=True)
    language_name = sa.Column(sa.String(), nullable=True)
    posting_day_holiday_flag = sa.Column(sa.Integer, nullable=True)
    settlement_day_holiday_flag = sa.Column(sa.Integer, nullable=True)
    ready_for_settlement_day_holiday_flag = sa.Column(sa.Integer, nullable=True)
    operating_day_holiday_flag = sa.Column(sa.Integer, nullable=True)
    media_type_id = sa.Column(sa.Integer, nullable=True)
    media_type_name = sa.Column(sa.String(), nullable=True)
    media_type_desc = sa.Column(sa.String(), nullable=True)
    media_class_id = sa.Column(sa.Integer, nullable=True)
    media_class_name = sa.Column(sa.String(), nullable=True)
    media_class_desc = sa.Column(sa.String(), nullable=True)
    ride_type_name = sa.Column(sa.String(), nullable=True)
    ride_type_desc = sa.Column(sa.String(), nullable=True)
    count_as_ride = sa.Column(sa.String(), nullable=True)
    count_as_entry = sa.Column(sa.String(), nullable=True)
    count_as_exit = sa.Column(sa.String(), nullable=True)
    free_entry_or_exit = sa.Column(sa.String(), nullable=True)
    forced_entry_or_exit = sa.Column(sa.String(), nullable=True)
    used_by_linked_trips_flag = sa.Column(sa.String(), nullable=True)
    count_as_transfer = sa.Column(sa.String(), nullable=True)
    sales_channel_name = sa.Column(sa.String(), nullable=True)
    sales_channel_desc = sa.Column(sa.String(), nullable=True)
    merchant_number = sa.Column(sa.String(), nullable=True)
    sale_type_name = sa.Column(sa.String(), nullable=True)
    sale_type_desc = sa.Column(sa.String(), nullable=True)
    count_as_sale = sa.Column(sa.Integer, nullable=True)
    txn_desc = sa.Column(sa.String(), nullable=True)
    txn_status_name = sa.Column(sa.String(), nullable=True)
    txn_status_desc = sa.Column(sa.String(), nullable=True)
    successful_sale_flag = sa.Column(sa.Integer, nullable=True)
    successful_use_flag = sa.Column(sa.Integer, nullable=True)
    device_name = sa.Column(sa.String(), nullable=True)
    device_desc = sa.Column(sa.String(), nullable=True)
    device_status_id = sa.Column(sa.Integer, nullable=True)
    operator_name = sa.Column(sa.String(), nullable=True)
    operator_desc = sa.Column(sa.String(), nullable=True)
    facility_name = sa.Column(sa.String(), nullable=True)
    facility_desc = sa.Column(sa.String(), nullable=True)
    business_entity_id = sa.Column(sa.String(), nullable=True)
    business_entity_name = sa.Column(sa.String(), nullable=True)
    bus_desc = sa.Column(sa.String(), nullable=True)
    bus_status_id = sa.Column(sa.Integer, nullable=True)
    bus_status_name = sa.Column(sa.String(), nullable=True)
    bus_status_desc = sa.Column(sa.String(), nullable=True)
    bus_type_id = sa.Column(sa.String(), nullable=True)
    bus_type_name = sa.Column(sa.String(), nullable=True)
    bus_type_desc = sa.Column(sa.String(), nullable=True)
    device_type_id = sa.Column(sa.Integer, nullable=True)
    device_type_name = sa.Column(sa.String(), nullable=True)
    device_type_desc = sa.Column(sa.String(), nullable=True)
    transit_mode_id = sa.Column(sa.Integer, nullable=True)
    transit_mode_name = sa.Column(sa.String(), nullable=True)
    transit_mode_desc = sa.Column(sa.String(), nullable=True)
    array_position = sa.Column(sa.Integer, nullable=True)
    fare_prod_id = sa.Column(sa.Integer, nullable=True)
    fare_prod_name = sa.Column(sa.String(), nullable=True)
    fare_prod_desc = sa.Column(sa.String(), nullable=True)
    fare_prod_rider_class_id = sa.Column(sa.Integer, nullable=True)
    fare_prod_rider_class_name = sa.Column(sa.String(), nullable=True)
    fare_prod_rider_class_desc = sa.Column(sa.String(), nullable=True)
    fare_prod_type_id = sa.Column(sa.Integer, nullable=True)
    fare_prod_type_name = sa.Column(sa.String(), nullable=True)
    fare_prod_type_desc = sa.Column(sa.String(), nullable=True)
    fare_prod_category_id = sa.Column(sa.Integer, nullable=True)
    fare_prod_category_name = sa.Column(sa.String(), nullable=True)
    fare_prod_category_desc = sa.Column(sa.String(), nullable=True)
    count_sale_as_ride_flag = sa.Column(sa.Integer, nullable=True)
    free_ride_flag = sa.Column(sa.String(), nullable=True)
    stop_point_id = sa.Column(sa.String(), nullable=True)
    stop_point_name = sa.Column(sa.String(), nullable=True)
    sector_id = sa.Column(sa.String(), nullable=True)
    sector_name = sa.Column(sa.String(), nullable=True)
    sector_desc = sa.Column(sa.String(), nullable=True)
    stop_point_display_desc = sa.Column(sa.String(), nullable=True)
    active_flag = sa.Column(sa.String(), nullable=True)
    stop_point_type_id = sa.Column(sa.String(), nullable=True)
    stop_point_type_name = sa.Column(sa.String(), nullable=True)
    stop_point_type_desc = sa.Column(sa.String(), nullable=True)
    line_route_id = sa.Column(sa.String(), nullable=True)
    service_type_id = sa.Column(sa.String(), nullable=True)
    service_type_name = sa.Column(sa.String(), nullable=True)
    service_type_desc = sa.Column(sa.String(), nullable=True)
    route_number = sa.Column(sa.String(), nullable=True)
    route_type_id = sa.Column(sa.String(), nullable=True)
    route_type_name = sa.Column(sa.String(), nullable=True)
    route_type_desc = sa.Column(sa.String(), nullable=True)
    route_name = sa.Column(sa.String(), nullable=True)
    route_desc = sa.Column(sa.String(), nullable=True)
    external_route_id = sa.Column(sa.String(), nullable=True)
    expiry_dtm_last_updated = sa.Column(sa.String(), nullable=True)
    init_dtm = sa.Column(sa.DateTime, nullable=True)
    issue_dtm = sa.Column(sa.DateTime, nullable=True)
    issue_seq_nbr = sa.Column(sa.String(), nullable=True)
    deposit_amount = sa.Column(sa.String(), nullable=True)
    rider_class_id = sa.Column(sa.Integer, nullable=True)
    rider_class_name = sa.Column(sa.String(), nullable=True)
    rider_class_desc = sa.Column(sa.String(), nullable=True)
    overdrawn_dtm = sa.Column(sa.DateTime, nullable=True)
    first_activation_dtm = sa.Column(sa.DateTime, nullable=True)
    first_registration_dtm = sa.Column(sa.DateTime, nullable=True)
    deposit_expiration_dtm = sa.Column(sa.String(), nullable=True)
    is_registered = sa.Column(sa.Integer, nullable=True)
    first_load_dtm = sa.Column(sa.DateTime, nullable=True)
    first_deposit_value = sa.Column(sa.Integer, nullable=True)
    first_load_type = sa.Column(sa.String(), nullable=True)
    total_loads = sa.Column(sa.Integer, nullable=True)
    total_pass_loads = sa.Column(sa.Integer, nullable=True)
    total_deposit_value = sa.Column(sa.Integer, nullable=True)
    total_active_passes = sa.Column(sa.Integer, nullable=True)
    first_tap_dtm = sa.Column(sa.DateTime, nullable=True)
    total_ride_count = sa.Column(sa.Integer, nullable=True)
    total_tap_count = sa.Column(sa.Integer, nullable=True)
    external_account_id = sa.Column(sa.Integer, nullable=True)
    dw_transaction_id = sa.Column(sa.String(), nullable=True)
    posting_day = sa.Column(sa.DateTime, nullable=True)
    settlement_day = sa.Column(sa.DateTime, nullable=True)
    bus_id = sa.Column(sa.Integer, nullable=True)
    account_adjustment_type_desc = sa.Column(sa.String(), nullable=True)
    account_value_changed = sa.Column(sa.String(), nullable=True)
    amount_short = sa.Column(sa.Integer, nullable=True)
    benefit_amount_used = sa.Column(sa.Integer, nullable=True)
    bonus_value = sa.Column(sa.Integer, nullable=True)
    value_changed = sa.Column(sa.Integer, nullable=True)
    value_remaining = sa.Column(sa.Integer, nullable=True)
    cash_collected = sa.Column(sa.Integer, nullable=True)
    device_id = sa.Column(sa.String(), nullable=True)
    device_position = sa.Column(sa.Integer, nullable=True)
    external_stop_point_id = sa.Column(sa.String(), nullable=True)
    facility_id = sa.Column(sa.Integer, nullable=True)
    fare_collected = sa.Column(sa.Integer, nullable=True)
    sale_group_id = sa.Column(sa.String(), nullable=True)
    latitude = sa.Column(sa.String(), nullable=True)
    longitude = sa.Column(sa.String(), nullable=True)
    merchant_id = sa.Column(sa.Integer, nullable=True)
    merchant_type = sa.Column(sa.String(), nullable=True)
    oa_account_id = sa.Column(sa.Integer, nullable=True)
    one_account_value = sa.Column(sa.Integer, nullable=True)
    operator_id = sa.Column(sa.Integer, nullable=True)
    over_payment = sa.Column(sa.Integer, nullable=True)
    over_payment_credit = sa.Column(sa.Integer, nullable=True)
    payment_types = sa.Column(sa.String(), nullable=True)
    prepaid_amount = sa.Column(sa.String(), nullable=True)
    postpaid_amount = sa.Column(sa.String(), nullable=True)
    refund_value = sa.Column(sa.String(), nullable=True)
    run_id = sa.Column(sa.String(), nullable=True)
    sale_transaction_type = sa.Column(sa.String(), nullable=True)
    sales_tax = sa.Column(sa.String(), nullable=True)
    serial_nbr = sa.Column(sa.Integer, nullable=True)
    transaction_dtm = sa.Column(sa.DateTime, nullable=True)
    transaction_status_id = sa.Column(sa.Integer, nullable=True)
    transaction_status_name = sa.Column(sa.String(), nullable=True)
    transit_account_id = sa.Column(sa.Integer, nullable=True)
    ready_for_settlement_day = sa.Column(sa.DateTime, nullable=True)
    operating_day = sa.Column(sa.DateTime, nullable=True)
    discount_amount = sa.Column(sa.Integer, nullable=True)
    _exported_dtm = sa.Column(sa.DateTime, nullable=True)
    promotion_id = sa.Column(sa.String(), nullable=True)
