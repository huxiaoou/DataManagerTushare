import time
import tushare as ts
import pandas as pd
from loguru import logger
from dataclasses import dataclass
from rich.progress import track
from husfort.qh5 import CDbHDF5
from husfort.qcalendar import CCalendar


@dataclass(frozen=True)
class CSaveDataInfo:
    db_name: str
    table_name_format: str
    desc: str
    fields: tuple[str, ...]


class __CDataEngineTushare:
    def __init__(self, save_root_dir: str, db_name: str, table_name_format: str, data_desc: str):
        self.api = ts.pro_api()
        self.save_root_dir = save_root_dir
        self.db_name = db_name
        self.table_name_format = table_name_format
        self.data_desc = data_desc

    def download_daily_data(self, trade_date: str) -> pd.DataFrame:
        raise NotImplementedError

    def download_data_range(self, bgn_date: str, stp_date: str, calendar: CCalendar):
        iter_dates = calendar.get_iter_list(bgn_date, stp_date)
        for trade_date in track(iter_dates, description=f"Downloading {self.data_desc}"):
            # for trade_date in iter_dates:
            h5lib = CDbHDF5(
                db_save_dir=self.save_root_dir,
                db_name=self.db_name,
                table=self.table_name_format.format(trade_date[0:4], trade_date[4:6], trade_date[6:8])
            )
            if h5lib.has_key() and (not h5lib.query_all().empty):
                logger.info(f"{self.data_desc} for {trade_date} exists, program will skip it")
            else:
                trade_date_data = self.download_daily_data(trade_date)
                h5lib.put(df=trade_date_data)
        return 0


class CDataEngineTushareFutDailyMd(__CDataEngineTushare):
    def __init__(self, save_root_dir: str, save_data_info: CSaveDataInfo):
        self.fields = ",".join(save_data_info.fields)
        super().__init__(
            save_root_dir=save_root_dir,
            db_name=save_data_info.db_name,
            table_name_format=save_data_info.table_name_format,
            data_desc=save_data_info.desc
        )

    def download_daily_data(self, trade_date: str) -> pd.DataFrame:
        while True:
            try:
                time.sleep(0.5)
                df = self.api.fut_daily(trade_date=trade_date, fields=self.fields)
                return df
            except TimeoutError as e:
                logger.error(e)
                time.sleep(5)
