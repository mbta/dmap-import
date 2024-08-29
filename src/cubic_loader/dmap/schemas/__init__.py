from sqlalchemy.orm import DeclarativeBase


# pylint: disable=too-few-public-methods
class SqlBase(DeclarativeBase):
    """
    SqlAlchemy Schema Base Class

    This design is recommended/expected for managing RDS Schema with SQLAlchemy
    in multiple files. With this design, alembic pickups of the SqlBase Metadata
    and is able to auto-generate schema migration files.

    pylint does not like this design and complains about order of imports and
    cyclic imports, so they are turned off for the individual schema imports
    """


# pylint: disable=cyclic-import
# pylint: disable=wrong-import-position
from .agg_average_boardings_by_day_type_month import (
    AggAvgBoardingsByDayTypeMonth,
)
from .agg_boardings_fareprod_mode_month import AggBoardingsFareprodModeMonth
from .agg_daily_fareprod_route import AggDailyFareprodRoute
from .agg_daily_fareprod_station import AggDailyFareprodStation
from .agg_daily_fareprod import AggDailyFareprod
from .agg_daily_transfers_route import AggDailyTransfersRoute
from .agg_daily_transfers_station import AggDailyTransfersStation
from .agg_hourly_entry_exit_count import AggHourlyEntryExitCount
from .agg_total_boardings_month_mode import AggTotalBoardingsMonthMode
from .device_event import DeviceEvents
from .sale_transaction import SaleTransaction
from .use_transaction_location import UseTransactionalLocation
from .use_transaction_longitudinal import UseTransactionalLongitudinal
from .api_metadata import ApiMetadata
