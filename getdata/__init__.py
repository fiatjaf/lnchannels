import sqlite3

from .schema import schema
from .listchannels import listchannels
from .listnodes import listnodes
from .enrich import enrich
from .checkcloses import checkcloses
from .closuretype import closuretype
from .materialize import materialize

db = sqlite3.connect("lnchannels.db", isolation_level=None)


def main():
    print("ensuring database")
    schema(db)

    print("inserting channels")
    listchannels(db)

    print("inserting nodes")
    listnodes(db)

    print("enriching")
    enrich(db)

    print("checking closes")
    checkcloses(db)

    print("determine closure type")
    closuretype(db)

    print("materialize")
    materialize(db)


main()
