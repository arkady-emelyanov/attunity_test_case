import argparse


def get_args() -> argparse.Namespace:
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-j", "--delta-library-jar", required=True, help="Delta library Jar path")
    arg_parser.add_argument("-l", "--load-path", required=True, help="Table load path")
    arg_parser.add_argument("-c", "--changes-path", required=True, help="Table changes path")
    arg_parser.add_argument("-s", "--snapshot-path", required=True, help="Table snapshot path")
    arg_parser.add_argument("-d", "--delta-path", required=True, help="Delta table path")
    arg_parser.add_argument("-i", "--delta-sdc-path", required=False, help="Delta table SDC path")
    cmd_args, _ = arg_parser.parse_known_args()
    return cmd_args
