from unittest import TestCase

from unittest.mock import MagicMock, call, patch

from gobbagextract.database.repository import MutationImport, MutationImportRepository


class MutationImportRepositoryTest(TestCase):

    @patch("gobbagextract.database.repository.MutationImport")
    def test_get_last(self, mock_mutation_import):
        session = MagicMock()
        repo = MutationImportRepository(session)

        res = repo.get_last('cat', 'coll', 'application')

        session.assert_has_calls([
            call.query(mock_mutation_import),
            call.query().filter_by(catalogue='cat', collection='coll', application='application'),
            call.query().filter_by().order_by(mock_mutation_import.started_at.desc()),
            call.query().filter_by().order_by().first(),
        ])

        self.assertEqual(session.query().filter_by().order_by().first(), res)

    def test_get(self):
        session = MagicMock()
        repo = MutationImportRepository(session)

        self.assertEqual(session.query.return_value.get.return_value, repo.get(42))
        session.assert_has_calls([
            call.query(MutationImport),
            call.query().get(42)
        ])

    def test_save(self):
        session = MagicMock()
        repo = MutationImportRepository(session)
        message = MutationImport()

        self.assertEqual(message, repo.save(message))
        session.assert_has_calls([
            call.add(message),
            call.flush(),
        ])
