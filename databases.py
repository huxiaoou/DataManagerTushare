import os
import pandas as pd
from rich.progress import track
from husfort.qutility import qtimer
from husfort.qinstruments import CInstrumentInfoTable
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
        for trade_date in track(iter_dates, description=f"Processing {self.raw_data_info.desc} to sql"):
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
        raw_data["instrument"] = raw_data["ts_code"].map(CInstrumentInfoTable.parse_instrument_from_contract)
        rft_data = raw_data[self.db_struct.table.vars.names]
        return rft_data
