from dataclasses import dataclass
from data_engines import CSaveDataInfo


@dataclass(frozen=True)
class CProCfg:
    calendar_path: str
    daily_data_root_dir: str
    futures_md: CSaveDataInfo


futures_md = CSaveDataInfo(
    file_format="tushare_futures_md_{}.csv.gz",
    desc="futures daily market data",
    fields=(
        "ts_code", "trade_date",
        "pre_close", "pre_settle",
        "open", "high", "low", "close", " settle",
        "vol", "amount", "oi"),
)

pro_cfg = CProCfg(
    calendar_path=r"E:\Deploy\Data\Calendar\cne_calendar.csv",
    daily_data_root_dir=r"D:\OneDrive\Data\tushare\by_date",
    futures_md=futures_md,
)
