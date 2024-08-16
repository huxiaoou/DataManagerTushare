import yaml
from dataclasses import dataclass
from data_engines import CSaveDataInfo
from husfort.qsqlite import CDbStruct, CSqlTable


# ---------- project configuration ----------

@dataclass(frozen=True)
class CProCfg:
    calendar_path: str
    root_dir: str
    daily_data_root_dir: str
    db_struct_path: str
    futures_exchanges: list[str]
    futures_md: CSaveDataInfo
    futures_contracts: CSaveDataInfo
    futures_universe: CSaveDataInfo
    futures_pos: CSaveDataInfo
    futures_basis: CSaveDataInfo
    futures_stock: CSaveDataInfo


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
    desc="futures daily basis",
    fields=("ts_code", "wd_code", "basis", "basis_rate", "basis_annual"),
)

futures_stock = CSaveDataInfo(
    file_format="wind_futures_stock_{}.csv.gz",
    desc="futures daily stock",
    fields=("ts_code", "wd_code", "stock"),
)

pro_cfg = CProCfg(
    calendar_path=r"D:\OneDrive\Data\Calendar\cne_calendar.csv",
    root_dir=r"D:\OneDrive\Data\tushare",
    daily_data_root_dir=r"D:\OneDrive\Data\tushare\by_date",
    db_struct_path=r"D:\OneDrive\Data\tushare\db_struct.yaml",
    futures_exchanges=["SHFE", "INE", "DCE", "CZCE", "GFEX", "CFFEX"],
    futures_md=futures_md,
    futures_contracts=futures_contracts,
    futures_universe=futures_universe,
    futures_pos=futures_pos,
    futures_basis=futures_basis,
    futures_stock=futures_stock,
)

# ---------- databases structure ----------
with open(pro_cfg.db_struct_path, "r") as f:
    db_struct = yaml.safe_load(f)


@dataclass(frozen=True)
class CDbStructCfg:
    fmd: CDbStruct
    position: CDbStruct
    basis: CDbStruct
    stock: CDbStruct


db_struct_cfg = CDbStructCfg(
    fmd=CDbStruct(
        db_save_dir=pro_cfg.root_dir,
        db_name=db_struct["fmd"]["db_name"],
        table=CSqlTable(cfg=db_struct["fmd"]["table"]),
    ),
    position=CDbStruct(
        db_save_dir=pro_cfg.root_dir,
        db_name=db_struct["position"]["db_name"],
        table=CSqlTable(cfg=db_struct["position"]["table"]),
    ),
    basis=CDbStruct(
        db_save_dir=pro_cfg.root_dir,
        db_name=db_struct["basis"]["db_name"],
        table=CSqlTable(cfg=db_struct["basis"]["table"]),
    ),
    stock=CDbStruct(
        db_save_dir=pro_cfg.root_dir,
        db_name=db_struct["stock"]["db_name"],
        table=CSqlTable(cfg=db_struct["stock"]["table"]),
    ),
)
