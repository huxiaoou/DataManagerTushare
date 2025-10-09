import os
import re
import pandas as pd
from rich.progress import track
from husfort.qutility import qtimer, SFY, SFG
from husfort.qinstruments import parse_instrument_from_contract
from husfort.qcalendar import CCalendar
from husfort.qsqlite import CDbStruct, CMgrSqlDb
from data_engines import CSaveDataInfo


class __CDbWriter:
    def __init__(self, db_struct: CDbStruct, raw_data_root_dir: str, raw_data_info: CSaveDataInfo):
        self.db_struct = db_struct
        self.save_root_dir = raw_data_root_dir
        self.raw_data_info = raw_data_info

    def load_data(self, trade_date: str) -> pd.DataFrame:
        raw_data_dir = os.path.join(self.save_root_dir, trade_date[0:4], trade_date)
        raw_data_file = self.raw_data_info.file_format.format(trade_date)
        raw_data_path = os.path.join(raw_data_dir, raw_data_file)
        raw_data = pd.read_csv(raw_data_path, dtype={"trade_date": str})
        return raw_data

    def reformat(self, raw_data: pd.DataFrame, trade_date: str) -> pd.DataFrame:
        raise NotImplementedError

    def to_sqldb(self, new_data: pd.DataFrame, calendar: CCalendar):
        sqldb = CMgrSqlDb(
            db_save_dir=self.db_struct.db_save_dir,
            db_name=self.db_struct.db_name,
            table=self.db_struct.table,
            mode="a",
            verbose=False
        )
        if sqldb.check_continuity(incoming_date=new_data["trade_date"].iloc[0], calendar=calendar) == 0:
            sqldb.update(update_data=new_data)
        return 0

    @qtimer
    def main(self, bgn_date: str, stp_date: str, calendar: CCalendar):
        iter_dates = calendar.get_iter_list(bgn_date, stp_date)
        new_data_list: list[pd.DataFrame] = []
        for trade_date in track(iter_dates, description=f"Processing {SFG(self.raw_data_info.desc)} to sql"):
            raw_data = self.load_data(trade_date)
            rft_data = self.reformat(raw_data, trade_date)
            new_data_list.append(rft_data)
        new_data = pd.concat(new_data_list, axis=0, ignore_index=True)
        self.to_sqldb(new_data, calendar)
        return 0


class CDbWriterFmd(__CDbWriter):
    def __init__(
            self,
            db_struct: CDbStruct, raw_data_root_dir: str, raw_data_info: CSaveDataInfo,
            cntrcts_data_info: CSaveDataInfo,
    ):
        self.cntrcts_data_info = cntrcts_data_info
        super().__init__(db_struct, raw_data_root_dir, raw_data_info)

    def load_cntrcts(self, trade_date: str) -> pd.DataFrame:
        cntrcts_data_dir = os.path.join(self.save_root_dir, trade_date[0:4], trade_date)
        cntrcts_data_file = self.cntrcts_data_info.file_format.format(trade_date)
        cntrcts_data_path = os.path.join(cntrcts_data_dir, cntrcts_data_file)
        cntrcts_data = pd.read_csv(cntrcts_data_path)
        return cntrcts_data

    def reformat(self, raw_data: pd.DataFrame, trade_date: str) -> pd.DataFrame:
        cntrcts_data = self.load_cntrcts(trade_date)
        raw_data = pd.merge(left=cntrcts_data, right=raw_data, left_on="contract", right_on="ts_code", how="left")
        raw_data["instrument"] = raw_data["ts_code"].map(parse_instrument_from_contract)
        rft_data = raw_data[self.db_struct.table.vars.names]
        return rft_data


class CDbWriterPos(__CDbWriter):
    @staticmethod
    def drop_symbols(raw_data: pd.DataFrame) -> pd.DataFrame:
        filter_rows = raw_data["symbol"].map(lambda _: not _.endswith("ACTV"))
        return raw_data[filter_rows].copy()

    @staticmethod
    def drop_nan_rows(raw_data: pd.DataFrame) -> pd.DataFrame:
        check_cols = ["broker", "vol", "vol_chg", "long_hld", "long_chg", "short_hld", "short_chg"]
        return raw_data.dropna(axis=0, subset=check_cols, how="all")

    @staticmethod
    def rft_broker(broker: str) -> str:
        return broker.replace("（代客）", "").replace("(代客)", "")

    @staticmethod
    def rft_symbol(symbol: str) -> str:
        x = re.sub(pattern=r"[^a-zA-Z0-9\.]", repl="", string=symbol)  # 仅保留a-z,A-Z,0-9 以及 . 即移除中文字符
        if x == "PTA":
            return "TA"
        elif x.startswith("si"):
            return x.upper()
        else:
            return x

    @staticmethod
    def rft_exchange(exchange: str) -> str:
        return {
            "SHFE": "SHF",
            "INE": "INE",
            "DCE": "DCE",
            "CZCE": "ZCE",
            "GFEX": "GFE",
            "CFFEX": "CFX",
        }[exchange]

    @staticmethod
    def parse_code_type(code: str, trade_date: str) -> int:
        if re.match(pattern=r"^[A-Z]{1,2}[\d]{4}\.[A-Z]{3}$", string=code) is not None:
            # format "XX0000.YYY" or "X0000.YYY"
            return 0
        elif re.match(pattern=r"^[A-Z]{1,2}\.[A-Z]{3}$", string=code) is not None:
            # format "XX.YYY" or "X.YYY"
            return 1
        else:
            raise ValueError(f"Pattern can not be parsed for code = {SFY(code)} @ {SFY(trade_date)}")

    def reformat(self, raw_data: pd.DataFrame, trade_date: str) -> pd.DataFrame:
        raw_data = self.drop_symbols(raw_data)
        raw_data = self.drop_nan_rows(raw_data)
        raw_data["broker"] = raw_data["broker"].map(self.rft_broker)
        raw_data["symbol"] = raw_data["symbol"].map(self.rft_symbol)
        raw_data["exchange"] = raw_data["exchange"].map(self.rft_exchange)
        raw_data["ts_code"] = raw_data[["symbol", "exchange"]].apply(lambda z: f"{z['symbol']}.{z['exchange']}", axis=1)
        raw_data["instrument"] = raw_data["ts_code"].map(parse_instrument_from_contract)
        raw_data["code_type"] = raw_data["ts_code"].map(lambda _: self.parse_code_type(_, trade_date))
        rft_data = raw_data[self.db_struct.table.vars.names]
        return rft_data


class CDbWriterBasis(__CDbWriter):
    def reformat(self, raw_data: pd.DataFrame, trade_date: str) -> pd.DataFrame:
        raw_data["trade_date"] = trade_date
        rft_data = raw_data[self.db_struct.table.vars.names]
        return rft_data


class CDbWriterStock(__CDbWriter):
    def reformat(self, raw_data: pd.DataFrame, trade_date: str) -> pd.DataFrame:
        raw_data["trade_date"] = trade_date
        rft_data = raw_data[self.db_struct.table.vars.names]
        return rft_data
