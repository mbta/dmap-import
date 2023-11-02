import sqlalchemy as sa
from dmap_import.schemas import SqlBase


class UseTransactionalLocation(SqlBase):
    """Table for A.3.2 USE TRANSACTIONAL – LOCATION-ENABLED"""

    # pylint: disable=too-few-public-methods
    # pylint: disable=R0801

    __tablename__ = "use_transaction_location"

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
    settlement_day_holiday_flag = sa.Column(sa.Integer, nullable=True)
    ready_for_settlement_day_holiday_flag = sa.Column(sa.Integer, nullable=True)
    posting_day_holiday_flag = sa.Column(sa.Integer, nullable=True)
    operating_day_holiday_flag = sa.Column(sa.Integer, nullable=True)
    media_type_id = sa.Column(sa.Integer, nullable=True)
    media_type_name = sa.Column(sa.String(), nullable=True)
    media_type_desc = sa.Column(sa.String(), nullable=True)
    media_class_id = sa.Column(sa.Integer, nullable=True)
    media_class_name = sa.Column(sa.String(), nullable=True)
    media_class_desc = sa.Column(sa.String(), nullable=True)
    ride_type_name = sa.Column(sa.String(), nullable=True)
    ride_type_desc = sa.Column(sa.String(), nullable=True)
    count_as_ride = sa.Column(sa.Integer, nullable=True)
    count_as_entry = sa.Column(sa.Integer, nullable=True)
    count_as_exit = sa.Column(sa.Integer, nullable=True)
    free_entry_or_exit = sa.Column(sa.Integer, nullable=True)
    forced_entry_or_exit = sa.Column(sa.Integer, nullable=True)
    used_by_linked_trips_flag = sa.Column(sa.Integer, nullable=True)
    count_as_transfer = sa.Column(sa.Integer, nullable=True)
    txn_desc = sa.Column(sa.String(), nullable=True)
    txn_status_name = sa.Column(sa.String(), nullable=True)
    txn_status_desc = sa.Column(sa.String(), nullable=True)
    successful_sale_flag = sa.Column(sa.Integer, nullable=True)
    successful_use_flag = sa.Column(sa.Integer, nullable=True)
    device_name = sa.Column(sa.String(), nullable=True)
    device_desc = sa.Column(sa.String(), nullable=True)
    device_status_id = sa.Column(sa.Integer, nullable=True)
    facility_name = sa.Column(sa.String(), nullable=True)
    facility_desc = sa.Column(sa.String(), nullable=True)
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
    array_position = sa.Column(sa.String(), nullable=True)
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
    operator_name = sa.Column(sa.String(), nullable=True)
    operator_desc = sa.Column(sa.String(), nullable=True)
    stop_point_id = sa.Column(sa.Float, nullable=True)
    stop_point_name = sa.Column(sa.String(), nullable=True)
    stop_point_sector_id = sa.Column(sa.Integer, nullable=True)
    stop_point_sector_name = sa.Column(sa.String(), nullable=True)
    stop_point_sector_desc = sa.Column(sa.String(), nullable=True)
    stop_point_display_desc = sa.Column(sa.String(), nullable=True)
    stop_point_active_flag = sa.Column(sa.Integer, nullable=True)
    stop_point_type_id = sa.Column(sa.Integer, nullable=True)
    stop_point_type_name = sa.Column(sa.String(), nullable=True)
    stop_point_type_desc = sa.Column(sa.String(), nullable=True)
    line_route_id = sa.Column(sa.Integer, nullable=True)
    route_number = sa.Column(sa.Float, nullable=True)
    route_type_id = sa.Column(sa.String(), nullable=True)
    route_type_name = sa.Column(sa.String(), nullable=True)
    route_type_desc = sa.Column(sa.String(), nullable=True)
    route_name = sa.Column(sa.String(), nullable=True)
    route_desc = sa.Column(sa.String(), nullable=True)
    external_route_id = sa.Column(sa.String(), nullable=True)
    rider_class_id = sa.Column(sa.Integer, nullable=True)
    rider_class_name = sa.Column(sa.String(), nullable=True)
    rider_class_desc = sa.Column(sa.String(), nullable=True)
    is_registered = sa.Column(sa.Integer, nullable=True)
    external_account_id = sa.Column(sa.Integer, nullable=True)
    posting_day = sa.Column(sa.DateTime, nullable=True)
    settlement_day = sa.Column(sa.DateTime, nullable=True)
    benefit_value = sa.Column(sa.Integer, nullable=True)
    bus_id = sa.Column(sa.Integer, nullable=True)
    calculated_fare = sa.Column(sa.Integer, nullable=True)
    device_id = sa.Column(sa.String(), nullable=True)
    device_position = sa.Column(sa.String(), nullable=True)
    external_stop_point_id = sa.Column(sa.String(), nullable=True)
    facility_id = sa.Column(sa.Integer, nullable=True)
    fare_due = sa.Column(sa.Integer, nullable=True)
    latitude = sa.Column(sa.Float, nullable=True)
    longitude = sa.Column(sa.Float, nullable=True)
    oa_account_id = sa.Column(sa.Integer, nullable=True)
    one_account_value = sa.Column(sa.Integer, nullable=True)
    operator_id = sa.Column(sa.Integer, nullable=True)
    patron_count = sa.Column(sa.String(), nullable=True)
    patron_trip_id = sa.Column(sa.Integer, nullable=True)
    reversed_dtm = sa.Column(sa.Integer, nullable=True)
    ride_count = sa.Column(sa.Integer, nullable=True)
    rides_remaining = sa.Column(sa.String(), nullable=True)
    run_id = sa.Column(sa.String(), nullable=True)
    serial_nbr = sa.Column(sa.Integer, nullable=True)
    service_type_id = sa.Column(sa.Integer, nullable=True)
    service_type_desc = sa.Column(sa.String(), nullable=True)
    service_type_name = sa.Column(sa.String(), nullable=True)
    transaction_dtm = sa.Column(sa.DateTime, nullable=True)
    transaction_status_id = sa.Column(sa.Integer, nullable=True)
    transaction_status_name = sa.Column(sa.String(), nullable=True)
    transfer_code = sa.Column(sa.String(), nullable=True)
    transfer_count = sa.Column(sa.Integer, nullable=True)
    transfer_flag = sa.Column(sa.Integer, nullable=True)
    transfer_sequence_nbr = sa.Column(sa.Integer, nullable=True)
    transit_account_id = sa.Column(sa.Integer, nullable=True)
    trip_info_trip_id = sa.Column(sa.String(), nullable=True)
    trip_info_additional_trip_flag = sa.Column(sa.String(), nullable=True)
    trip_info_inserted_trip_flag = sa.Column(sa.String(), nullable=True)
    trip_info_trip_destn_sign_id = sa.Column(sa.String(), nullable=True)
    trip_info_trip_block_num = sa.Column(sa.String(), nullable=True)
    trip_info_location = sa.Column(sa.String(), nullable=True)
    trip_info_direction = sa.Column(sa.String(), nullable=True)
    trip_price_count = sa.Column(sa.Integer, nullable=True)
    value_changed = sa.Column(sa.Integer, nullable=True)
    value_remaining = sa.Column(sa.String(), nullable=True)
    ready_for_settlement_day = sa.Column(sa.DateTime, nullable=True)
    voided_dtm = sa.Column(sa.Integer, nullable=True)
    operating_day = sa.Column(sa.DateTime, nullable=True)
    fare_rule_description = sa.Column(sa.String(), nullable=True)
    _exported_dtm = sa.Column(sa.DateTime, nullable=True)
    promotion_id = sa.Column(sa.String(), nullable=True)
