import os
import sys
import time
import tushare as ts
import pandas as pd
import re
from WindPy import w as wapi
from loguru import logger
from husfort.qutility import check_and_makedirs, qtimer, SFG
from husfort.qcalendar import CCalendar
from dataclasses import dataclass
from rich.progress import track

pd.set_option('display.unicode.east_asian_width', True)


@dataclass(frozen=True)
class CSaveDataInfo:
    file_format: str
    desc: str
    fields: tuple[str, ...]


class __CDataEngine:
    def __init__(self, save_root_dir: str, save_file_format: str, data_desc: str):
        self.save_root_dir = save_root_dir
        self.save_file_format = save_file_format
        self.data_desc = data_desc

    def download_daily_data(self, trade_date: str) -> pd.DataFrame:
        raise NotImplementedError

    @qtimer
    def download_data_range(self, bgn_date: str, stp_date: str, calendar: CCalendar):
        iter_dates = calendar.get_iter_list(bgn_date, stp_date)
        for trade_date in track(iter_dates, description=f"Downloading {SFG(self.data_desc)}"):
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


class __CDataEngineTushare(__CDataEngine):
    def __init__(self, save_root_dir: str, save_file_format: str, data_desc: str):
        self.api = ts.pro_api()
        super().__init__(save_root_dir, save_file_format, data_desc)


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


class CDataEngineTushareFutDailyCntrcts(__CDataEngine):
    def __init__(self, save_root_dir: str, save_data_info: CSaveDataInfo, md_data_info: CSaveDataInfo):
        super().__init__(save_root_dir, save_data_info.file_format, save_data_info.desc)
        self.md_data_info = md_data_info

    @staticmethod
    def is_contract(symbol: str) -> bool:
        # try to match "CH2409.SHF"
        return re.match(pattern=r"^[A-Z]{1,2}[\d]{4}\.[A-Z]{3}$", string=symbol) is not None

    def download_daily_data(self, trade_date: str) -> pd.DataFrame:
        md_dir = os.path.join(self.save_root_dir, trade_date[0:4], trade_date)
        md_file = self.md_data_info.file_format.format(trade_date)
        md_path = os.path.join(md_dir, md_file)
        md = pd.read_csv(md_path)
        contracts = filter(self.is_contract, md["ts_code"])
        df = pd.DataFrame({"contract": contracts})
        return df


class CDataEngineTushareFutDailyUnvrs(__CDataEngine):
    def __init__(self, save_root_dir: str, save_data_info: CSaveDataInfo, cntrcts_data_info: CSaveDataInfo,
                 exceptions: set[str]):
        super().__init__(save_root_dir, save_data_info.file_format, save_data_info.desc)
        self.cntrcts_data_info = cntrcts_data_info
        self.exceptions: set[str] = exceptions

    @staticmethod
    def to_instrument(symbol: str) -> str:
        return re.sub(pattern="[0-9]", repl="", string=symbol)

    @staticmethod
    def to_wind_code(symbol: str) -> str:
        return symbol.replace(".ZCE", ".CZC").replace(".CFX", ".CFE")

    def download_daily_data(self, trade_date: str) -> pd.DataFrame:
        cntrcts_dir = os.path.join(self.save_root_dir, trade_date[0:4], trade_date)
        cntrcts_file = self.cntrcts_data_info.file_format.format(trade_date)
        cntrcts_path = os.path.join(cntrcts_dir, cntrcts_file)
        cntrcts = pd.read_csv(cntrcts_path)["contract"]
        universe_ts = list(set(map(self.to_instrument, cntrcts)) - self.exceptions)
        universe_wd = [self.to_wind_code(_) for _ in universe_ts]
        df = pd.DataFrame({
            "ts_code": universe_ts,
            "wd_code": universe_wd,
        }).sort_values("ts_code")
        return df


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


class __CDataEngineWind(__CDataEngine):
    def __init__(self, save_root_dir: str, save_file_format: str, data_desc: str, unvrs_data_info: CSaveDataInfo):
        self.api = wapi
        self.api.start()
        self.unvrs_data_info = unvrs_data_info
        super().__init__(save_root_dir, save_file_format, data_desc)

    @staticmethod
    def convert_data_to_dataframe(downloaded_data, download_values: list[str], col_names: list[str]) -> pd.DataFrame:
        if downloaded_data.ErrorCode != 0:
            logger.error(f"When download data from WIND, ErrorCode = {downloaded_data.ErrorCode}.")
            logger.info("Program will terminate at once, please check again.")
            sys.exit()
        else:
            df = pd.DataFrame(downloaded_data.Data, index=download_values, columns=col_names).T
            return df

    def load_universe(self, trade_date: str) -> pd.DataFrame:
        unvrs_file = self.unvrs_data_info.file_format.format(trade_date)
        unvrs_dir = os.path.join(self.save_root_dir, trade_date[0:4], trade_date)
        unvrs_path = os.path.join(unvrs_dir, unvrs_file)
        unvrs_data = pd.read_csv(unvrs_path)
        return unvrs_data


class CDataEngineWindFutDailyBasis(__CDataEngineWind):
    def __init__(self, save_root_dir: str, save_data_info: CSaveDataInfo, unvrs_data_info: CSaveDataInfo):
        super().__init__(save_root_dir, save_data_info.file_format, save_data_info.desc, unvrs_data_info)

    def download_daily_data(self, trade_date: str) -> pd.DataFrame:
        while True:
            try:
                time.sleep(0.5)
                universe = self.load_universe(trade_date)
                universe["isInCFE"] = universe["wd_code"].map(lambda _: _.split(".")[1] == "CFE")
                unvrs_f = universe.loc[universe["isInCFE"], "wd_code"].tolist()
                unvrs_c = universe.loc[~universe["isInCFE"], "wd_code"].tolist()

                # download financial
                indicators = {
                    "anal_basis_stkidx": "basis",
                    "anal_basispercent_stkidx": "basis_rate",
                    "anal_basisannualyield_stkidx": "basis_annual",
                }
                f_data = self.api.wss(codes=unvrs_f, fields=list(indicators), options=f"tradeDate={trade_date}")
                df_f = self.convert_data_to_dataframe(f_data, download_values=list(indicators), col_names=unvrs_f)
                df_f = df_f.rename(mapper=indicators, axis=1)

                # download commodity
                indicators = {
                    "anal_basis": "basis",
                    "anal_basispercent2": "basis_rate",
                    "basisannualyield": "basis_annual",
                }
                c_data = self.api.wss(codes=unvrs_c, fields=list(indicators), options=f"tradeDate={trade_date}")
                df_c = self.convert_data_to_dataframe(c_data, download_values=list(indicators), col_names=unvrs_c)
                df_c = df_c.rename(mapper=indicators, axis=1)

                # concat
                df = pd.concat([df_f, df_c], axis=0, ignore_index=False)
                res = pd.merge(
                    left=universe[["ts_code", "wd_code"]],
                    right=df,
                    left_on="wd_code",
                    right_index=True,
                    how="left",
                )
                return res
            except TimeoutError as e:
                logger.error(e)
                time.sleep(5)
