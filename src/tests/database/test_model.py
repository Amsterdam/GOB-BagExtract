from unittest import TestCase

import datetime

from gobbagextract.database.model import MutationImport


class MutationImportTest(TestCase):

    def test_is_ended(self):
        mi = MutationImport()

        self.assertFalse(mi.is_ended())
        mi.ended_at = datetime.datetime.utcnow()
        self.assertTrue(mi.is_ended())

    def test_repr(self):
        mi = MutationImport()
        mi.catalogue = "CAT"
        mi.collection = "COLL"
        mi.filename = "FNAME"

        self.assertEqual("<MutationImport CAT COLL (FNAME)>", str(mi))
