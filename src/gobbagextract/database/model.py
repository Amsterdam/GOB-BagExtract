import datetime

from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class MutationImport(Base):
    __tablename__ = "mutation_import"

    id = Column(Integer, primary_key=True)
    catalogue = Column(String, doc='The catalogue', index=True)
    collection = Column(String, doc='The collection', index=True)
    application = Column(String, doc='The application', index=True)
    started_at = Column(DateTime, doc='Start time', default=datetime.datetime.utcnow)
    ended_at = Column(DateTime, doc='End time')
    filename = Column(String, doc='The filename associated with this import')
    mode = Column(String, doc='Mutation or full')

    def is_ended(self):
        return self.ended_at is not None

    def __repr__(self):
        return f'<MutationImport {self.catalogue} {self.collection} ({self.filename})>'
