from gobbagextract.database.model import MutationImport


class MutationImportRepository:

    def __init__(self, session):
        self.session = session

    def get_last(self, catalogue: str, collection: str, application: str):
        return self.session\
            .query(MutationImport)\
            .filter_by(catalogue=catalogue, collection=collection, application=application)\
            .order_by(MutationImport.started_at.desc())\
            .first()

    def save(self, obj: MutationImport):
        self.session.add(obj)
        self.session.flush()
        return obj

    def get(self, id: int) -> MutationImport:
        return self.session.query(MutationImport).get(id)
