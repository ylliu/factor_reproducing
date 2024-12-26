from unittest import TestCase

from LNCAP.Incap_data_collect import get_hs300_constituents


class Test(TestCase):
    def test_get_hs300_constituents(self):
        start_date = "20090123"  # 开始日期
        end_date = "20190430"  # 结束日期
        print(get_hs300_constituents(start_date, end_date))
