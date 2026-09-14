"""Renaming views

Revision ID: 42e39526183d
Revises: 4b9e25ccbea7
Create Date: 2026-09-10 12:13:28.564684

"""

from typing import Sequence, Union

from alembic import op

from cubic_loader.utils.postgres import DatabaseManager

# revision identifiers, used by Alembic.
revision: str = "42e39526183d"
down_revision: Union[str, None] = "4b9e25ccbea7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SCHEMA = "ods"

# (old_name, new_name) for plain views (views.py and comp_views.py)
VIEW_RENAMES = [
    ("farerev_prod_sales_txn_a", "comp_a_addendum_farerev_prod_sales_txn_a"),
    ("farerev_prod_sales_txn_c", "comp_a_farerev_prod_sales_txn_c"),
    ("farerev_payg_trip_txn_a", "comp_b_addendum_farerev_payg_trip_txn_a"),
    ("farerev_payg_trip_txn_c", "comp_b_farerev_payg_trip_txn_c"),
    ("farerev_recovery_txn_a", "comp_c_addendum_farerev_recovery_txn_a"),
    ("farerev_recovery_txn_c", "comp_c_farerev_recovery_txn_c"),
    ("wa160", "use_txns_wa160"),
    ("wo110", "patron_order_details_wo110"),
    ("wo118", "product_transfer_details_wo118"),
    ("wo150", "csr_patron_adjustments_wo150"),
]

# (old_name, new_name) for materialized views (mat_views.py)
MAT_VIEW_RENAMES: list[tuple[str, str]] = []


def existing_views(db: DatabaseManager, catalog: str, name_column: str) -> set[str]:
    query = f"SELECT {name_column} FROM pg_catalog.{catalog} WHERE schemaname = '{SCHEMA}';"
    return {row[name_column] for row in db.select_as_list(query)}


def rename_view(old_name: str, new_name: str, materialized: bool) -> None:
    view_kind = "MATERIALIZED VIEW" if materialized else "VIEW"
    op.execute(f"ALTER {view_kind} {SCHEMA}.{old_name} RENAME TO {new_name};")


def upgrade() -> None:
    db = DatabaseManager()
    schema_check_query = "SELECT COUNT(*) from information_schema.tables WHERE table_schema = 'ods';"
    if db.select(schema_check_query)["count"] == 0:
        return

    # Fail before rename if any views are not found / incorrectly specified here
    views = existing_views(db, "pg_views", "viewname")
    mat_views = existing_views(db, "pg_matviews", "matviewname")
    missing = [name for name, _ in VIEW_RENAMES if name not in views]
    missing += [name for name, _ in MAT_VIEW_RENAMES if name not in mat_views]
    if missing:
        raise RuntimeError(f"views not found in schema '{SCHEMA}': {', '.join(missing)}")

    for old_name, new_name in VIEW_RENAMES:
        rename_view(old_name, new_name, materialized=False)

    for old_name, new_name in MAT_VIEW_RENAMES:
        rename_view(old_name, new_name, materialized=True)


def downgrade() -> None:
    for old_name, new_name in reversed(MAT_VIEW_RENAMES):
        rename_view(new_name, old_name, materialized=True)

    for old_name, new_name in reversed(VIEW_RENAMES):
        rename_view(new_name, old_name, materialized=False)
