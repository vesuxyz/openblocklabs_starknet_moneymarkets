import logging
import os
import subprocess
from argparse import ArgumentParser, Namespace, RawTextHelpFormatter


def get_args() -> Namespace:
    parser = ArgumentParser(
        formatter_class=RawTextHelpFormatter
    )
    
    parser.add_argument(
        "--protocol",
        type=str,
        required=True,
        help="The name of the protocol folder."
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()
    logging.info(f"Running Protocol: {args.protocol}...")

    # Path to the 'protocols' directory
    protocols_directory = f"protocols/{args.protocol}"

    # Walk through all subdirectories in 'protocols'
    for dirpath, dirnames, filenames in os.walk(protocols_directory):
        for filename in filenames:
            if filename == "function.py":  # Check if the filename is exactly 'function.py'
                # Construct the full file path
                file_path = os.path.join(dirpath, filename)

                # Run the Python script
                print(f"Running {file_path}...")
                subprocess.run(["python3", file_path])
