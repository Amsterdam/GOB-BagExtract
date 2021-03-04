"""
DatastoreToPostgresSelector

Contains logic to run a (select) query on an oracle database and import this in a new table in Postgres.
"""
from gobbagextract.selector._selector import Selector
from gobbagextract.selector._from_datastore import FromDatastoreSelector
from gobbagextract.selector._to_postgres import ToPostgresSelector


class DatastoreToPostgresSelector(Selector, FromDatastoreSelector, ToPostgresSelector):
    pass
