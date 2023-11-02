import time

import sqlalchemy
import alembic

from dmap_import.util_rds import alembic_upgrade_to_head


def start() -> None:
    """
    start main event loop of application
    """
    print("hello world")
    print(f"using SQLAlchemy Version {sqlalchemy.__version__}")
    print(f"using alembic Version {alembic.__version__}")

    alembic_upgrade_to_head()

    while True:
        print("sleeping for 30s. zzzzzzzz")
        time.sleep(30)


def main() -> None:
    """
    initialize and validate environment, then start running the application
    """
    # do some validation
    a = 1
    assert a == 1

    # start up the process
    start()


if __name__ == "__main__":
    main()
