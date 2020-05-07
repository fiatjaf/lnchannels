import psycopg2

from .globals import POSTGRES_URL
from .listchannels import listchannels
from .inspectblocks import inspectblocks
from .listnodes import listnodes
from .chain_analysis import chain_analysis


def main():
    with psycopg2.connect(POSTGRES_URL) as conn:
        conn.autocommit = True

        with conn.cursor() as db:
            print("listing channels")
            listchannels(db)

        with conn.cursor() as db:
            print("inspecting blocks")
            inspectblocks(db)

        with conn.cursor() as db:
            print("inserting nodes")
            listnodes(db)

        with conn.cursor() as db:
            print("performing chain analysis")
            chain_analysis(db)

        with conn.cursor() as db:
            db.execute("REFRESH MATERIALIZED VIEW last_block")
            db.execute("REFRESH MATERIALIZED VIEW implementations")
            db.execute("REFRESH MATERIALIZED VIEW nodes")
            db.execute("REFRESH MATERIALIZED VIEW globalstats")
            db.execute("REFRESH MATERIALIZED VIEW closetypes")


main()
