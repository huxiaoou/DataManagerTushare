import os
import time
import tushare as ts
import pandas as pd
from loguru import logger
from husfort.qutility import check_and_makedirs
from husfort.qcalendar import CCalendar
from dataclasses import dataclass
from rich.progress import track


@dataclass(frozen=True)
class CSaveDataInfo:
    file_format: str
    desc: str
    fields: tuple[str, ...]


class __CDataEngineTushare:
    def __init__(self, save_root_dir: str, save_file_format: str, data_desc: str):
        self.api = ts.pro_api()
        self.save_root_dir = save_root_dir
        self.save_file_format = save_file_format
        self.data_desc = data_desc

    def download_daily_data(self, trade_date: str) -> pd.DataFrame:
        raise NotImplementedError

    def download_data_range(self, bgn_date: str, stp_date: str, calendar: CCalendar):
        iter_dates = calendar.get_iter_list(bgn_date, stp_date)
        for trade_date in track(iter_dates, description=f"Downloading {self.data_desc}"):
            # for trade_date in iter_dates:
            check_and_makedirs(save_dir := os.path.join(self.save_root_dir, trade_date[0:4], trade_date))
            save_file = self.save_file_format.format(trade_date)
            save_path = os.path.join(save_dir, save_file)
            if os.path.exists(save_path):
                logger.info(f"{self.data_desc} for {trade_date} exists, program will skip it")
            else:
                trade_date_data = self.download_daily_data(trade_date)
                trade_date_data.to_csv(save_path, index=False)
        return 0


class CDataEngineTushareFutDailyMd(__CDataEngineTushare):
    def __init__(self, save_root_dir: str, save_data_info: CSaveDataInfo):
        self.fields = ",".join(save_data_info.fields)
        super().__init__(save_root_dir, save_data_info.file_format, save_data_info.desc)

    def download_daily_data(self, trade_date: str) -> pd.DataFrame:
        while True:
            try:
                time.sleep(0.5)
                df = self.api.fut_daily(trade_date=trade_date, fields=self.fields)
                return df
            except TimeoutError as e:
                logger.error(e)
                time.sleep(5)
