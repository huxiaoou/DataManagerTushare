from dataclasses import dataclass
from data_engines import CSaveDataInfo


@dataclass(frozen=True)
class CProCfg:
    calendar_path: str
    daily_data_root_dir: str
    futures_md: CSaveDataInfo


futures_md = CSaveDataInfo(
    db_name="tushare_futures_by_date.h5",
    table_name_format="Y{}/M{}/D{}/md",
    desc="futures daily market data",
    fields=(
        "ts_code", "trade_date",
        "pre_close", "pre_settle",
        "open", "high", "low", "close", " settle",
        "vol", "amount", "oi"),
)

pro_cfg = CProCfg(
    calendar_path=r"E:\Deploy\Data\Calendar\cne_calendar.csv",
    daily_data_root_dir=r"D:\OneDrive\Data\tushare",
    futures_md=futures_md,
)
