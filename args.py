import argparse


def get_arguments():
    """
    Gets the required arguments (source and destination titles) from the command line.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("src_title")
    parser.add_argument("dst_title")
    parsed_args = parser.parse_args()
    return parsed_args.src_title, parsed_args.dst_title
