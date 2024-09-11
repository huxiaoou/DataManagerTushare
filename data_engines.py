import os
import sys
import re
import zipfile
import time
import datetime as dt
import tushare as ts
import pandas as pd
import multiprocessing as mp
from loguru import logger
from dataclasses import dataclass
from rich.progress import Progress, TaskID
from WindPy import w as wapi
from husfort.qutility import check_and_makedirs, qtimer, SFG, SFR, error_handler
from husfort.qcalendar import CCalendar

pd.set_option('display.unicode.east_asian_width', True)
logger.add("logs/download_and_update.log")


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

    def download_daily_data(self, trade_date: str, task_id: TaskID, pb: Progress) -> pd.DataFrame:
        raise NotImplementedError

    @qtimer
    def download_data_range(self, bgn_date: str, stp_date: str, calendar: CCalendar):
        iter_dates = calendar.get_iter_list(bgn_date, stp_date)
        with Progress() as pb:
            task_pri = pb.add_task(description="Pri-task description to be updated", total=len(iter_dates))
            task_sub = pb.add_task(description="Sub-task description to be updated")
            for trade_date in iter_dates:
                pb.update(task_id=task_pri, description=f"Processing data for {SFG(trade_date)}")
                check_and_makedirs(save_dir := os.path.join(self.save_root_dir, trade_date[0:4], trade_date))
                save_file = self.save_file_format.format(trade_date)
                save_path = os.path.join(save_dir, save_file)
                if os.path.exists(save_path):
                    logger.info(f"{self.data_desc} for {trade_date} exists, program will skip it")
                else:
                    trade_date_data = self.download_daily_data(trade_date, task_id=task_sub, pb=pb)
                    trade_date_data.to_csv(save_path, index=False)
                pb.update(task_id=task_pri, advance=1)
        return 0


class __CDataEngineTushare(__CDataEngine):
    def __init__(self, save_root_dir: str, save_file_format: str, data_desc: str):
        self.api = ts.pro_api()
        super().__init__(save_root_dir, save_file_format, data_desc)


class CDataEngineTushareFutDailyMd(__CDataEngineTushare):
    def __init__(self, save_root_dir: str, save_data_info: CSaveDataInfo):
        self.fields = ",".join(save_data_info.fields)
        super().__init__(save_root_dir, save_data_info.file_format, save_data_info.desc)

    def download_daily_data(self, trade_date: str, task_id: TaskID, pb: Progress) -> pd.DataFrame:
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

    def download_daily_data(self, trade_date: str, task_id: TaskID, pb: Progress) -> pd.DataFrame:
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

    def download_daily_data(self, trade_date: str, task_id: TaskID, pb: Progress) -> pd.DataFrame:
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


class CDataEngineTushareFutDailyMinuteBar(__CDataEngine):
    def __init__(self, save_root_dir: str, save_data_info: CSaveDataInfo,
                 md_data_info: CSaveDataInfo, cntrcts_data_info: CSaveDataInfo,
                 tick_data_root_dir: str, calendar: CCalendar, top: int = 3,
                 ):
        """

        :param save_root_dir:
        :param save_data_info:
        :param md_data_info:
        :param cntrcts_data_info: make sure contracts data for trade date has been created
        :param tick_data_root_dir: like 'E:\\OneDrive\\Data\\juejindata'
        :param calendar:
        :param top: how many contracts of each instrument will be downloaded for minute data
        """

        self.md_data_info = md_data_info
        self.cntrcts_data_info = cntrcts_data_info
        self.fields = ",".join(save_data_info.fields)
        self.tick_data_root_dir = tick_data_root_dir
        self.calendar = calendar
        self.top = top
        super().__init__(save_root_dir, save_data_info.file_format, save_data_info.desc)

    def load_md(self, trade_date) -> pd.DataFrame:
        md_dir = os.path.join(self.save_root_dir, trade_date[0:4], trade_date)
        md_file = self.md_data_info.file_format.format(trade_date)
        md_path = os.path.join(md_dir, md_file)
        md = pd.read_csv(md_path)
        return md

    @staticmethod
    def reformat_md(md: pd.DataFrame) -> pd.DataFrame:
        md = md.rename(columns={"ts_code": "contract"})
        md["vol"] = md["vol"].fillna(0)
        md = md[["contract", "vol"]]
        return md

    def load_cntrcts(self, trade_date) -> pd.DataFrame:
        cntrcts_dir = os.path.join(self.save_root_dir, trade_date[0:4], trade_date)
        cntrcts_file = self.cntrcts_data_info.file_format.format(trade_date)
        cntrcts_path = os.path.join(cntrcts_dir, cntrcts_file)
        cntrcts = pd.read_csv(cntrcts_path)
        return cntrcts

    @staticmethod
    def add_instrument(data: pd.DataFrame, contract_name: str = "contract") -> None:
        data["instrument"] = data[contract_name].map(lambda _: re.sub(pattern=r"\d", repl="", string=_))

    def find_top_cntrcts(self, md_cntrcts: pd.DataFrame) -> dict[str, list[str]]:
        top_cntrcts_for_instru: dict[str, list[str]] = {}  # type:ignore
        for instru, instru_data in md_cntrcts.groupby(by="instrument"):
            top_cntrcts_for_instru[instru] = instru_data.head(self.top)["contract"].tolist()  # type:ignore
        return top_cntrcts_for_instru

    @staticmethod
    def reformat_contract(contract: str) -> tuple[str, str]:
        contract_ctp, exchange = contract.split(".")
        if exchange == "ZCE":
            return contract_ctp[:-4] + contract_ctp[-3:], exchange
        elif exchange == "CFX":
            return contract_ctp, exchange
        elif exchange in ["DCE", "SHF", "INE", "GFE"]:
            return contract_ctp.lower(), exchange

    def load_contract_file_from_zipfile(self, new_contract: str, trade_date: str) -> pd.DataFrame:
        zip_path = os.path.join(self.tick_data_root_dir, trade_date[0:4], f"{trade_date[0:6]}.zip")
        with zipfile.ZipFile(zip_path, mode="r") as zf:
            files = zf.namelist()
            target_file = f"{trade_date[0:6]}/{trade_date}/{new_contract}_{trade_date}.csv"
            if target_file in files:
                sf = zf.open(target_file)
                tick_data = pd.read_csv(sf)
                return tick_data
            else:
                logger.info(f"{SFR(target_file)} is not found.")
                return pd.DataFrame()

    @staticmethod
    def cal_vol_and_to(tick_data: pd.DataFrame) -> None:
        tick_data["Volume"] = tick_data["Volume"] - tick_data["Volume"].shift(1).fillna(0)
        tick_data["Turnover"] = tick_data["Turnover"] - tick_data["Turnover"].shift(1).fillna(0)

    def generate_minute_bar(self, instru: str, contract: str, trade_date: str) -> pd.DataFrame:
        contract_ctp, exchange = self.reformat_contract(contract)
        tick_data = self.load_contract_file_from_zipfile(contract_ctp, trade_date)
        if tick_data.empty:
            return pd.DataFrame()
        self.cal_vol_and_to(tick_data)
        tick_parser = CTickDataParser(
            trade_date, contract=contract, instru=instru, exchange=exchange,
            save_vars=self.fields.split(","), calendar=self.calendar,
        )
        rft_data = tick_parser.main(tick_data=tick_data)
        return rft_data

    def download_daily_data(self, trade_date: str, task_id: TaskID, pb: Progress) -> pd.DataFrame:
        md = self.reformat_md(self.load_md(trade_date))
        cntrcts = self.load_cntrcts(trade_date)
        md_cntrcts = pd.merge(left=cntrcts, right=md, on="contract", how="left")
        self.add_instrument(md_cntrcts)
        md_cntrcts = md_cntrcts.sort_values(by=["instrument", "vol", "contract"], ascending=[True, False, True])
        top_cntrcts_for_instru = self.find_top_cntrcts(md_cntrcts)
        iter_args: list[tuple[str, str]] = []
        for instru, contracts in top_cntrcts_for_instru.items():
            for contract in contracts:
                iter_args.append((instru, contract))
        pb.update(task_id, total=len(iter_args), description=f"Processing contracts of {SFG(trade_date)}", completed=0)
        with mp.get_context("spawn").Pool() as pool:
            jobs = []
            for instru, contract in iter_args:
                job = pool.apply_async(
                    self.generate_minute_bar,
                    args=(instru, contract, trade_date),
                    callback=lambda _: pb.update(task_id, advance=1),
                    error_callback=error_handler,
                )
                jobs.append(job)
            pool.close()
            pool.join()
        dfs: list[pd.DataFrame] = [job.get() for job in jobs]
        minute_bar_data = pd.concat(dfs, axis=0, ignore_index=True)
        return minute_bar_data


class CDataEngineTushareFutDailyPos(__CDataEngineTushare):
    def __init__(self, save_root_dir: str, save_data_info: CSaveDataInfo, exchanges: list[str]):
        self.fields = ",".join(save_data_info.fields)
        self.exchanges = exchanges
        super().__init__(save_root_dir, save_data_info.file_format, save_data_info.desc)

    def download_daily_data(self, trade_date: str, task_id: TaskID, pb: Progress) -> pd.DataFrame:
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

    def download_daily_data(self, trade_date: str, task_id: TaskID, pb: Progress) -> pd.DataFrame:
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


class CDataEngineWindFutDailyStock(__CDataEngineWind):
    def __init__(self, save_root_dir: str, save_data_info: CSaveDataInfo, unvrs_data_info: CSaveDataInfo):
        super().__init__(save_root_dir, save_data_info.file_format, save_data_info.desc, unvrs_data_info)

    def download_daily_data(self, trade_date: str, task_id: TaskID, pb: Progress) -> pd.DataFrame:
        while True:
            try:
                time.sleep(0.5)
                universe = self.load_universe(trade_date)
                unvrs = universe["wd_code"].tolist()
                indicators = {"st_stock": "stock"}
                stock_data = self.api.wss(codes=unvrs, fields=list(indicators), options=f"tradeDate={trade_date}")
                df = self.convert_data_to_dataframe(stock_data, download_values=list(indicators), col_names=unvrs)
                df = df.rename(mapper=indicators, axis=1)
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


# --- tick data aggregate ---
class CTickDataParser:
    EQT_TRADE_TIME_CHG_DATE = "20160101"

    def __init__(
            self, trade_date: str, contract: str, instru: str, exchange: str,
            save_vars: list[str], calendar: CCalendar,
    ):
        self.contract = contract
        self.instru, self.exchange = instru, exchange
        self.save_vars = save_vars
        self.this_trade_date = trade_date
        self.prev_trade_date = calendar.get_next_date(self.this_trade_date, shift=-1)
        self.t_date = dt.datetime.strptime(self.this_trade_date, "%Y%m%d")
        self.p_date = dt.datetime.strptime(self.prev_trade_date, "%Y%m%d")
        self.l_date = self.p_date + dt.timedelta(days=1)
        self.tail_trade_date = self.l_date.strftime("%Y%m%d")

    def __parse_date_from_time(self, t_time: str):
        if t_time <= "04:00:00":
            return self.tail_trade_date
        elif t_time <= "16:00:00":
            return self.this_trade_date
        else:
            return self.prev_trade_date

    def add_trade_date(self, tick_data: pd.DataFrame) -> None:
        tick_data["trade_date"] = tick_data["UpdateTime"].map(self.__parse_date_from_time)

    @staticmethod
    def add_ticks(tick_data: pd.DataFrame) -> None:
        tick_data["ts"] = tick_data[["trade_date", "UpdateTime", "UpdateMillisec"]].apply(
            lambda z: f"{z['trade_date']} {z['UpdateTime']}.{z['UpdateMillisec']:03d}", axis=1)
        # tick_data["ts"] = tick_data["ts"].map(lambda _: dt.datetime.strptime(_, "%Y%m%d %H:%M:%S.%f"))
        tick_data["ts"] = pd.to_datetime(tick_data["ts"])
        tick_data.set_index(keys="ts", inplace=True)

    @staticmethod
    def __revise_to_end(db: dt.datetime, de: dt.datetime, i: int, timestamp: dt.datetime, s: list[dt.datetime]) -> bool:
        if db <= timestamp <= de:
            s[i] = de
            return True
        return False

    @staticmethod
    def __revise_to_bgn(db: dt.datetime, de: dt.datetime, i: int, timestamp: dt.datetime, s: list[dt.datetime]) -> bool:
        if db <= timestamp <= de:
            s[i] = db - dt.timedelta(milliseconds=1)
            return True
        return False

    def __revise_non_cfx(self, tick_data: pd.DataFrame) -> pd.DataFrame:
        # night
        d0b = self.p_date + dt.timedelta(hours=20, minutes=59)
        d0e = self.p_date + dt.timedelta(hours=21, minutes=0)

        d1b = self.l_date + dt.timedelta(hours=2, minutes=30)
        d1e = self.l_date + dt.timedelta(hours=2, minutes=35)

        # morning
        d2b = self.t_date + dt.timedelta(hours=8, minutes=59)
        d2e = self.t_date + dt.timedelta(hours=9, minutes=0)

        d3b = self.t_date + dt.timedelta(hours=10, minutes=15)
        d3e = self.t_date + dt.timedelta(hours=10, minutes=16)

        # middle
        d4b = self.t_date + dt.timedelta(hours=10, minutes=29)
        d4e = self.t_date + dt.timedelta(hours=10, minutes=30)

        d5b = self.t_date + dt.timedelta(hours=11, minutes=30)
        d5e = self.t_date + dt.timedelta(hours=11, minutes=31)

        # afternoon
        d6b = self.t_date + dt.timedelta(hours=13, minutes=29)
        d6e = self.t_date + dt.timedelta(hours=13, minutes=30)

        d7b = self.t_date + dt.timedelta(hours=15, minutes=0)
        d7e = self.t_date + dt.timedelta(hours=15, minutes=5)

        ts_lst = tick_data.index.tolist()
        for i, timestamp in enumerate(ts_lst):
            if self.__revise_to_end(d0b, d0e, i, timestamp, ts_lst):
                continue
            if self.__revise_to_bgn(d1b, d1e, i, timestamp, ts_lst):
                continue
            if self.__revise_to_end(d2b, d2e, i, timestamp, ts_lst):
                continue
            if self.__revise_to_bgn(d3b, d3e, i, timestamp, ts_lst):
                continue
            if self.__revise_to_end(d4b, d4e, i, timestamp, ts_lst):
                continue
            if self.__revise_to_bgn(d5b, d5e, i, timestamp, ts_lst):
                continue
            if self.__revise_to_end(d6b, d6e, i, timestamp, ts_lst):
                continue
            if self.__revise_to_bgn(d7b, d7e, i, timestamp, ts_lst):
                continue
        tick_data.index = ts_lst
        truncated_data = tick_data.query(
            f"(index >= '{d0e}' & index < '{d1b}')  | (index >= '{d2e}' & index < '{d7b}')"
        )
        return truncated_data

    def __revise_cfx_equity(self, tick_data: pd.DataFrame) -> pd.DataFrame:
        # morning section
        if self.this_trade_date < self.EQT_TRADE_TIME_CHG_DATE:
            d0b = self.t_date + dt.timedelta(hours=9, minutes=10)
            d0e = self.t_date + dt.timedelta(hours=9, minutes=15)
        else:
            d0b = self.t_date + dt.timedelta(hours=9, minutes=25)
            d0e = self.t_date + dt.timedelta(hours=9, minutes=30)

        d1b = self.t_date + dt.timedelta(hours=11, minutes=30)
        d1e = self.t_date + dt.timedelta(hours=11, minutes=31)

        # afternoon section
        d2b = self.t_date + dt.timedelta(hours=12, minutes=59)
        d2e = self.t_date + dt.timedelta(hours=13, minutes=0)
        if self.this_trade_date < self.EQT_TRADE_TIME_CHG_DATE:
            d3b = self.t_date + dt.timedelta(hours=15, minutes=15)
            d3e = self.t_date + dt.timedelta(hours=15, minutes=20)
        else:
            d3b = self.t_date + dt.timedelta(hours=15, minutes=0)
            d3e = self.t_date + dt.timedelta(hours=15, minutes=5)

        ts_lst = tick_data.index.tolist()
        for i, timestamp in enumerate(ts_lst):
            if self.__revise_to_end(d0b, d0e, i, timestamp, ts_lst):
                continue
            if self.__revise_to_bgn(d1b, d1e, i, timestamp, ts_lst):
                continue
            if self.__revise_to_end(d2b, d2e, i, timestamp, ts_lst):
                continue
            if self.__revise_to_bgn(d3b, d3e, i, timestamp, ts_lst):
                continue
        tick_data.index = ts_lst
        truncated_data = tick_data.query(f"index >= '{d0e}' & index < '{d3b}'")
        return truncated_data

    def __revise_cfx_treasury_bond(self, tick_data: pd.DataFrame) -> pd.DataFrame:
        # morning section
        d0b = self.t_date + dt.timedelta(hours=9, minutes=10)
        d0e = self.t_date + dt.timedelta(hours=9, minutes=15)
        d1b = self.t_date + dt.timedelta(hours=11, minutes=30)
        d1e = self.t_date + dt.timedelta(hours=11, minutes=31)

        # afternoon section
        d2b = self.t_date + dt.timedelta(hours=12, minutes=59)
        d2e = self.t_date + dt.timedelta(hours=13, minutes=0)
        d3b = self.t_date + dt.timedelta(hours=15, minutes=15)
        d3e = self.t_date + dt.timedelta(hours=15, minutes=20)

        ts_lst = tick_data.index.tolist()
        for i, timestamp in enumerate(ts_lst):
            if self.__revise_to_end(d0b, d0e, i, timestamp, ts_lst):
                continue
            if self.__revise_to_bgn(d1b, d1e, i, timestamp, ts_lst):
                continue
            if self.__revise_to_end(d2b, d2e, i, timestamp, ts_lst):
                continue
            if self.__revise_to_bgn(d3b, d3e, i, timestamp, ts_lst):
                continue
        tick_data.index = ts_lst
        truncated_data = tick_data.query(f"index >= '{d0e}' & index < '{d3b}'")
        return truncated_data

    def revise_ticks(self, tick_data: pd.DataFrame) -> pd.DataFrame:
        if self.exchange == "CFX":
            if self.instru.upper() in ["IH.CFX", "IF.CFX", "IC.CFX", "IM.CFX"]:
                return self.__revise_cfx_equity(tick_data)
            elif self.instru.upper() in ["TS.CFX", "TF.CFX", "T.CFX", "TL.CFX"]:
                return self.__revise_cfx_treasury_bond(tick_data)
            else:
                raise ValueError(f"instru = {SFR(self.instru)} is illegal for CFX")
        else:
            return self.__revise_non_cfx(tick_data)

    @staticmethod
    def agg_tick_data_to_bar(tick_data: pd.DataFrame) -> pd.DataFrame:
        ohlc_data = tick_data["LastPrice"].resample("1min").ohlc()
        vol_data = tick_data[["Volume", "Turnover", "OpenInterest"]].resample("1min").aggregate({
            "Volume": "sum",
            "Turnover": "sum",
            "OpenInterest": "last",
        })
        bar_data = pd.merge(left=ohlc_data, right=vol_data, left_index=True, right_index=True, how="inner")
        return bar_data

    def reformat_bar(self, bar_data: pd.DataFrame) -> pd.DataFrame:
        rft_data = bar_data.dropna(axis=0, subset=["open", "high", "low", "close", "OpenInterest"])
        rft_data = rft_data.reset_index().rename(
            columns={"Volume": "vol", "Turnover": "amount", "OpenInterest": "oi", "index": "timestamp"}
        )
        rft_data["ts_code"] = self.contract
        rft_data["trade_date"] = self.this_trade_date
        rft_data = rft_data[self.save_vars]
        return rft_data

    def main(self, tick_data: pd.DataFrame) -> pd.DataFrame:
        self.add_trade_date(tick_data)
        self.add_ticks(tick_data)
        truncated_data = self.revise_ticks(tick_data)
        bar_data = self.agg_tick_data_to_bar(truncated_data)
        rft_data = self.reformat_bar(bar_data)
        return rft_data
