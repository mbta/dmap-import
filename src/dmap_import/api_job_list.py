import os
from typing import TypedDict, List, Any, Union
from sqlalchemy.orm import DeclarativeBase

from dmap_import.schemas.agg_average_boardings_by_day_type_month import (
    AggAvgBoardingsByDayTypeMonth,
)
from dmap_import.schemas.agg_boardings_fareprod_mode_month import (
    AggBoardingsFareprodModeMonth,
)
from dmap_import.schemas.agg_daily_fareprod_route import AggDailyFareprodRoute
from dmap_import.schemas.agg_daily_fareprod_station import (
    AggDailyFareprodStation,
)
from dmap_import.schemas.agg_daily_fareprod import AggDailyFareprod
from dmap_import.schemas.agg_daily_transfers_route import AggDailyTransfersRoute
from dmap_import.schemas.agg_daily_transfers_station import (
    AggDailyTransfersStation,
)
from dmap_import.schemas.agg_hourly_entry_exit_count import (
    AggHourlyEntryExitCount,
)
from dmap_import.schemas.agg_total_boardings_month_mode import (
    AggTotalBoardingsMonthMode,
)
from dmap_import.schemas.device_event import DeviceEvents
from dmap_import.schemas.sale_transaction import SaleTransaction
from dmap_import.schemas.use_transaction_location import (
    UseTransactionalLocation,
)
from dmap_import.schemas.use_transaction_longitudinal import (
    UseTransactionalLongitudinal,
)


class CopyJob(TypedDict):
    """
    CUBIC API Copy Job

    url: Full CUBIC API Endpoint URL
    table: SQLAlchemy schema object for API Endpoint destination table
    """

    url: str
    table: Union[DeclarativeBase, Any]


def produce_job_list() -> List[CopyJob]:
    """Make list of CUBIC API Copy jobs to run"""
    base_url = os.getenv("DMAP_BASE_URL")

    return [
        {
            "url": f"{base_url}/datasetpublicusersapi/aggregations/agg_average_boardings_by_day_type_month",
            "table": AggAvgBoardingsByDayTypeMonth,
        },
        {
            "url": f"{base_url}/datasetpublicusersapi/aggregations/agg_boardings_fareprod_mode_month",
            "table": AggBoardingsFareprodModeMonth,
        },
        {
            "url": f"{base_url}/datasetpublicusersapi/aggregations/agg_daily_fareprod_route",
            "table": AggDailyFareprodRoute,
        },
        {
            "url": f"{base_url}/datasetpublicusersapi/aggregations/agg_daily_fareprod_station",
            "table": AggDailyFareprodStation,
        },
        {
            "url": f"{base_url}/datasetpublicusersapi/aggregations/agg_daily_fareprod",
            "table": AggDailyFareprod,
        },
        {
            "url": f"{base_url}/datasetpublicusersapi/aggregations/agg_daily_transfers_route",
            "table": AggDailyTransfersRoute,
        },
        {
            "url": f"{base_url}/datasetpublicusersapi/aggregations/agg_daily_transfers_station",
            "table": AggDailyTransfersStation,
        },
        {
            "url": f"{base_url}/datasetpublicusersapi/aggregations/agg_hourly_entry_exit_count",
            "table": AggHourlyEntryExitCount,
        },
        {
            "url": f"{base_url}/datasetpublicusersapi/aggregations/agg_total_boardings_month_mode",
            "table": AggTotalBoardingsMonthMode,
        },
        {
            "url": f"{base_url}/datasetcontrolleduserapi/transactional/use_transaction_longitudinal",
            "table": UseTransactionalLongitudinal,
        },
        {
            "url": f"{base_url}/datasetcontrolleduserapi/transactional/use_transaction_location",
            "table": UseTransactionalLocation,
        },
        {
            "url": f"{base_url}/datasetcontrolleduserapi/transactional/sale_transaction",
            "table": SaleTransaction,
        },
        {
            "url": f"{base_url}/datasetcontrolleduserapi/transactional/device_event",
            "table": DeviceEvents,
        },
    ]
