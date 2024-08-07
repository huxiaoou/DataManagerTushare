import argparse


def parse_args():
    arg_parser = argparse.ArgumentParser(description="Project to download data from tushare")
    arg_parser.add_argument(
        "--switch", type=str, required=True,
        choices=("fmd", "contract", "universe", "position"),
    )
    arg_parser.add_argument("--bgn", type=str, required=True)
    arg_parser.add_argument("--stp", type=str, default=None)
    _args = arg_parser.parse_args()
    return _args


if __name__ == "__main__":
    from project_cfg import pro_cfg
    from husfort.qcalendar import CCalendar
    from husfort.qlog import define_logger

    define_logger()

    args = parse_args()
    calendar = CCalendar(calendar_path=pro_cfg.calendar_path)

    if args.switch == "fmd":
        from data_engines import CDataEngineTushareFutDailyMd

        engine = CDataEngineTushareFutDailyMd(
            save_root_dir=pro_cfg.daily_data_root_dir,
            save_data_info=pro_cfg.futures_md,
        )
        engine.download_data_range(
            bgn_date=args.bgn,
            stp_date=args.stp or calendar.get_next_date(args.bgn),
            calendar=calendar,
        )
    elif args.switch == "contract":
        from data_engines import CDataEngineTushareFutDailyCntrcts

        engine = CDataEngineTushareFutDailyCntrcts(
            save_root_dir=pro_cfg.daily_data_root_dir,
            save_data_info=pro_cfg.futures_contracts,
            md_data_info=pro_cfg.futures_md,
        )
        engine.download_data_range(
            bgn_date=args.bgn,
            stp_date=args.stp or calendar.get_next_date(args.bgn),
            calendar=calendar,
        )
    elif args.switch == "universe":
        from data_engines import CDataEngineTushareFutDailyUnvrs

        engine = CDataEngineTushareFutDailyUnvrs(
            save_root_dir=pro_cfg.daily_data_root_dir,
            save_data_info=pro_cfg.futures_universe,
            cntrcts_data_info=pro_cfg.futures_contracts,
            exceptions={"SCTAS.INE"},
        )
        engine.download_data_range(
            bgn_date=args.bgn,
            stp_date=args.stp or calendar.get_next_date(args.bgn),
            calendar=calendar,
        )
    elif args.switch == "position":
        from data_engines import CDataEngineTushareFutDailyPos

        engine = CDataEngineTushareFutDailyPos(
            save_root_dir=pro_cfg.daily_data_root_dir,
            save_data_info=pro_cfg.futures_pos,
            exchanges=pro_cfg.futures_exchanges,
        )
        engine.download_data_range(
            bgn_date=args.bgn,
            stp_date=args.stp or calendar.get_next_date(args.bgn),
            calendar=calendar,
        )
    else:
        raise ValueError(f"switch = {args.switch} is illegal")
