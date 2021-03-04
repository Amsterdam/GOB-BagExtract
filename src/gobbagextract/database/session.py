from sqlalchemy.orm import sessionmaker

from gobbagextract.database.connection import engine


class DatabaseSession:
    """DatabaseSession

    Provides a context manager with a session to the database.
    """
    Session = None

    def __init__(self):
        self.session = None

        if self.Session is None:
            self._init_sessionmaker()

    @classmethod
    def _init_sessionmaker(cls):
        cls.Session = sessionmaker(autocommit=True, bind=engine)

    def __enter__(self):
        self.session = self.Session()
        return self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.session.rollback()
        else:
            self.session.flush()
        self.session.expunge_all()
        self.session.close()
        self.session = None
