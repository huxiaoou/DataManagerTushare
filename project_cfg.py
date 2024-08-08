from dataclasses import dataclass
from data_engines import CSaveDataInfo


@dataclass(frozen=True)
class CProCfg:
    calendar_path: str
    daily_data_root_dir: str
    futures_exchanges: list[str]
    futures_md: CSaveDataInfo
    futures_contracts: CSaveDataInfo
    futures_universe: CSaveDataInfo
    futures_pos: CSaveDataInfo
    futures_basis: CSaveDataInfo


futures_md = CSaveDataInfo(
    file_format="tushare_futures_md_{}.csv.gz",
    desc="futures daily market data",
    fields=(
        "ts_code", "trade_date",
        "pre_close", "pre_settle",
        "open", "high", "low", "close", " settle",
        "vol", "amount", "oi"),
)

futures_contracts = CSaveDataInfo(
    file_format="tushare_futures_contracts_{}.csv.gz",
    desc="futures daily contracts",
    fields=("contract",),
)

futures_universe = CSaveDataInfo(
    file_format="tushare_futures_universe_{}.csv.gz",
    desc="futures daily universe",
    fields=("ts_code", "wd_code"),
)

futures_pos = CSaveDataInfo(
    file_format="tushare_futures_pos_{}.csv.gz",
    desc="futures daily holding positions",
    fields=(
        "trade_date", "symbol",
        "broker", "vol", "vol_chg", "long_hld", "long_chg", "short_hld", "short_chg",
        "exchange",
    ),
)

futures_basis = CSaveDataInfo(
    file_format="wind_futures_basis_{}.csv.gz",
    desc="futures daily fundamental data",
    fields=("ts_code", "wd_code", "basis", "basis_rate", "basis_annual"),
)

pro_cfg = CProCfg(
    calendar_path=r"E:\Deploy\Data\Calendar\cne_calendar.csv",
    daily_data_root_dir=r"D:\OneDrive\Data\tushare\by_date",
    futures_exchanges=["SHFE", "INE", "DCE", "CZCE", "GFEX", "CFFEX"],
    futures_md=futures_md,
    futures_contracts=futures_contracts,
    futures_universe=futures_universe,
    futures_pos=futures_pos,
    futures_basis=futures_basis,
)
