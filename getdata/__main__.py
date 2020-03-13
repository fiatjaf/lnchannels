import os
import psycopg2

from .schema import schema
from .listchannels import listchannels
from .listnodes import listnodes
from .enrich import enrich
from .checkcloses import checkcloses
from .closuretype import closuretypes
from .materialize import materialize

POSTGRES_URL = os.getenv("POSTGRES_URL")


def main():
    with psycopg2.connect(POSTGRES_URL) as conn:
        conn.autocommit = True

        with conn.cursor() as db:
            print("ensuring database")
            schema(db)

        with conn.cursor() as db:
            print("inserting channels")
            listchannels(db)

        with conn.cursor() as db:
            print("inserting nodes")
            listnodes(db)

        with conn.cursor() as db:
            print("enriching")
            enrich(db)

        with conn.cursor() as db:
            print("checking closes")
            checkcloses(db)

        with conn.cursor() as db:
            print("determine closure type")
            closuretypes(db)

        with conn.cursor() as db:
            print("materialize")
            materialize(db)


main()
