import os
import time
import tushare as ts
import pandas as pd
import re
from loguru import logger
from husfort.qutility import check_and_makedirs, qtimer
from husfort.qcalendar import CCalendar
from dataclasses import dataclass
from rich.progress import track

pd.set_option('display.unicode.east_asian_width', True)


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

    @qtimer
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


class CDataEngineTushareFutDailyCntrcts(__CDataEngineTushare):
    def __init__(self, save_root_dir: str, save_data_info: CSaveDataInfo, md_data_info: CSaveDataInfo):
        self.fields = ",".join(save_data_info.fields)
        super().__init__(save_root_dir, save_data_info.file_format, save_data_info.desc)
        self.md_data_info = md_data_info

    @staticmethod
    def is_contract(symbol: str) -> bool:
        return re.match(pattern=r"^[A-Z]+[0-9]{4}\.[A-Z]{3}", string=symbol) is not None

    def download_daily_data(self, trade_date: str) -> pd.DataFrame:
        while True:
            try:
                md_dir = os.path.join(self.save_root_dir, trade_date[0:4], trade_date)
                md_file = self.md_data_info.file_format.format(trade_date)
                md_path = os.path.join(md_dir, md_file)
                md = pd.read_csv(md_path)
                contracts = filter(self.is_contract, md["ts_code"])
                df = pd.DataFrame({"contract": contracts})
                return df
            except TimeoutError as e:
                logger.error(e)
                time.sleep(5)


class CDataEngineTushareFutDailyPos(__CDataEngineTushare):
    def __init__(self, save_root_dir: str, save_data_info: CSaveDataInfo, exchanges: list[str]):
        self.fields = ",".join(save_data_info.fields)
        self.exchanges = exchanges
        super().__init__(save_root_dir, save_data_info.file_format, save_data_info.desc)

    def download_daily_data(self, trade_date: str) -> pd.DataFrame:
        while True:
            try:
                dfs: list[pd.DataFrame] = []
                for exchange in self.exchanges:
                    time.sleep(0.2)
                    exchange_data = self.api.fut_holding(
                        trade_date=trade_date,
                        exchange=exchange,
                        fields=self.fields,
                    )
                    if not exchange_data.empty:
                        dfs.append(exchange_data)
                df = pd.concat(dfs, axis=0, ignore_index=True)
                return df
            except TimeoutError as e:
                logger.error(e)
                time.sleep(5)
