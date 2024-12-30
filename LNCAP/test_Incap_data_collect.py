import re
from unittest import TestCase

from LNCAP.Incap_data_collect import StockDataProcessor


class TestStockDataProcessor(TestCase):

    def test_get_industry_data(self):
        token = "c632fdfffc18c3b68ba65351dde6638ff8ff147d6033b12da8d2be86"  # 替换为你的Tushare token
        start_date = "20090123"
        end_date = "20190430"
        processor = StockDataProcessor(token, start_date, end_date)
        df = processor.process_data()
        print(df)
