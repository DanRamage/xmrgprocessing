from datetime import datetime
from types import ModuleType
import sys
import unittest

sys.modules.setdefault("requests", ModuleType("requests"))

from xmrgprocessing.xmrgfileiterator.xmrg_file_iterator import xmrg_file_iterator


class XmrgFileIteratorDateRangeTests(unittest.TestCase):
    def test_iterates_hourly_paths_until_exclusive_end_date(self):
        iterator = xmrg_file_iterator(
            start_date=datetime(2024, 5, 1, 22),
            end_date=datetime(2024, 5, 2, 1),
            base_xmrg_path="/data/xmrg",
        )

        self.assertEqual(
            list(iterator),
            [
                "/data/xmrg/2024/May/xmrg0501202422z.gz",
                "/data/xmrg/2024/May/xmrg0501202423z.gz",
                "/data/xmrg/2024/May/xmrg0502202400z.gz",
            ],
        )

    def test_iterates_paths_for_provided_date_list(self):
        iterator = xmrg_file_iterator(
            date_list=[
                datetime(2024, 5, 1, 22),
                datetime(2024, 5, 3, 7),
            ],
            base_xmrg_path="/data/xmrg",
        )

        self.assertEqual(
            list(iterator),
            [
                "/data/xmrg/2024/May/xmrg0501202422z.gz",
                "/data/xmrg/2024/May/xmrg0503202407z.gz",
            ],
        )


if __name__ == "__main__":
    unittest.main()
