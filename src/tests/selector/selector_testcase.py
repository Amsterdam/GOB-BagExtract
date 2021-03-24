from unittest import TestCase

from gobbagextract.selector._selector import Selector


class SelectorTestCase(TestCase):

    def assertHasMethod(self, o, name):
        self.assertTrue(callable(getattr(o, name, None)))

    def assertValidSelector(self, selector):
        # Assert child of "Selector"
        self.assertTrue(isinstance(selector, Selector))

        # Assert have "ToSelector" (methods needed by Selector)
        self.assertHasMethod(selector, "_write_rows")
        self.assertHasMethod(selector, "_create_destination_table")
        self.assertHasMethod(selector, "_prepare_row")

        # Assert have "FromSelector" (methods needed by Selector)
        self.assertHasMethod(selector, "_read_rows")
