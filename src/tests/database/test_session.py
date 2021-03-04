from unittest import TestCase
from unittest.mock import patch, MagicMock, call

from gobbagextract.database.session import DatabaseSession


@patch("gobbagextract.database.session.sessionmaker")
class TestDatabaseSession(TestCase):

    def setUp(self) -> None:
        DatabaseSession.Session = None

    @patch("gobbagextract.database.session.engine")
    def test_init(self, mock_engine, mock_sessionmaker):
        ds = DatabaseSession()

        self.assertEqual(mock_sessionmaker.return_value, ds.Session)
        mock_sessionmaker.assert_called_with(autocommit=True, bind=mock_engine)

        mock_sessionmaker.reset_mock()
        ds = DatabaseSession()
        mock_sessionmaker.assert_not_called()
        self.assertIsNotNone(ds.Session)
        self.assertIsNone(ds.session)

    def test_enter(self, mock_sessionmaker):
        ds = DatabaseSession()
        res = ds.__enter__()

        self.assertEqual(ds.session, res)
        self.assertEqual(mock_sessionmaker.return_value.return_value, res)

    def test_exit(self, mock_sessionmaker):
        ds = DatabaseSession()
        session = MagicMock()
        ds.session = session
        ds.__exit__(None, None, None)

        session.assert_has_calls([
            call.flush(),
            call.expunge_all(),
            call.close(),
        ])
        session.rollback.assert_not_called()
        self.assertIsNone(ds.session)

        session.reset_mock()
        ds.session = session

        ds.__exit__('exc type', 'exc val', 'exc tb')
        session.assert_has_calls([
            call.rollback(),
            call.expunge_all(),
            call.close()
        ])
        session.flush.assert_not_called()
        self.assertIsNone(ds.session)
