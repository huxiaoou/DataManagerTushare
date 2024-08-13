import argparse


def parse_args():
    arg_parser_main = argparse.ArgumentParser(description="Project to download data from tushare")
    arg_parser_main.add_argument("--bgn", type=str, required=True)
    arg_parser_main.add_argument("--stp", type=str, default=None)

    arg_parser_subs = arg_parser_main.add_subparsers(
        title="sub function",
        dest="func",
        description="use this argument to go to call different functions",
    )

    # func: download
    arg_parser_sub = arg_parser_subs.add_parser(name="download", help="Download data from tushare and wind")
    arg_parser_sub.add_argument(
        "--switch", type=str, required=True,
        choices=("fmd", "contract", "universe", "position", "basis"),
    )

    # func: update
    arg_parser_sub = arg_parser_subs.add_parser(name="update", help="Update data for database")
    arg_parser_sub.add_argument(
        "--switch", type=str, required=True,
        choices=("fmd", "position", "basis"),
    )

    # --- parse args
    _args = arg_parser_main.parse_args()
    return _args


if __name__ == "__main__":
    from project_cfg import pro_cfg
    from husfort.qlog import define_logger
    from husfort.qcalendar import CCalendar

    define_logger()
    calendar = CCalendar(calendar_path=pro_cfg.calendar_path)

    args = parse_args()
    bgn, stp = args.bgn, args.stp or calendar.get_next_date(args.bgn, shift=1)

    if args.func == "download":
        if args.switch == "fmd":
            from data_engines import CDataEngineTushareFutDailyMd

            engine = CDataEngineTushareFutDailyMd(
                save_root_dir=pro_cfg.daily_data_root_dir,
                save_data_info=pro_cfg.futures_md,
            )
            engine.download_data_range(bgn_date=bgn, stp_date=stp, calendar=calendar)
        elif args.switch == "contract":
            from data_engines import CDataEngineTushareFutDailyCntrcts

            engine = CDataEngineTushareFutDailyCntrcts(
                save_root_dir=pro_cfg.daily_data_root_dir,
                save_data_info=pro_cfg.futures_contracts,
                md_data_info=pro_cfg.futures_md,
            )
            engine.download_data_range(bgn_date=bgn, stp_date=stp, calendar=calendar)
        elif args.switch == "universe":
            from data_engines import CDataEngineTushareFutDailyUnvrs

            engine = CDataEngineTushareFutDailyUnvrs(
                save_root_dir=pro_cfg.daily_data_root_dir,
                save_data_info=pro_cfg.futures_universe,
                cntrcts_data_info=pro_cfg.futures_contracts,
                exceptions={"SCTAS.INE"},
            )
            engine.download_data_range(bgn_date=bgn, stp_date=stp, calendar=calendar)
        elif args.switch == "position":
            from data_engines import CDataEngineTushareFutDailyPos

            engine = CDataEngineTushareFutDailyPos(
                save_root_dir=pro_cfg.daily_data_root_dir,
                save_data_info=pro_cfg.futures_pos,
                exchanges=pro_cfg.futures_exchanges,
            )
            engine.download_data_range(bgn_date=bgn, stp_date=stp, calendar=calendar)
        elif args.switch == "basis":
            from data_engines import CDataEngineWindFutDailyBasis

            engine = CDataEngineWindFutDailyBasis(
                save_root_dir=pro_cfg.daily_data_root_dir,
                save_data_info=pro_cfg.futures_basis,
                unvrs_data_info=pro_cfg.futures_universe,
            )
            engine.download_data_range(bgn_date=bgn, stp_date=stp, calendar=calendar)
        else:
            raise ValueError(f"switch = {args.switch} is illegal")
    elif args.func == "update":
        if args.switch == "fmd":
            from databases import CDbWriterFmd
            from project_cfg import pro_cfg, db_struct_cfg

            sqldb_writer = CDbWriterFmd(
                db_struct=db_struct_cfg.fmd,
                raw_data_root_dir=pro_cfg.daily_data_root_dir,
                raw_data_info=pro_cfg.futures_md,
                cntrcts_data_info=pro_cfg.futures_contracts,
            )
            sqldb_writer.main(bgn_date=bgn, stp_date=stp, calendar=calendar)
        elif args.switch == "position":
            from databases import CDbWriterPos
            from project_cfg import pro_cfg, db_struct_cfg

            sqldb_writer = CDbWriterPos(
                db_struct=db_struct_cfg.position,
                raw_data_root_dir=pro_cfg.daily_data_root_dir,
                raw_data_info=pro_cfg.futures_pos,
            )
            sqldb_writer.main(bgn_date=bgn, stp_date=stp, calendar=calendar)
        elif args.switch == "basis":
            pass
        else:
            raise ValueError(f"switch = {args.switch} is illegal")
