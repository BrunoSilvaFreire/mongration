import argparse
import sys
import pathlib


def main():
    # Create the parser
    parser = argparse.ArgumentParser(description='Mongrate: A tool for managing database migrations.')

    # Add the --mongration argument
    parser.add_argument('--mongration', type=str, help='Path to the mongration script to be executed.', required=True)

    # Parse the command line arguments
    args = parser.parse_args()
    path = pathlib.Path(__file__)
    main_dir = str(path.parent.parent.absolute())
    if main_dir not in sys.path:
        sys.path.append(main_dir)
    # Call the mongrate function with the provided mongration script path
    from mongrations.loading import run_mongration
    run_mongration(args)


if __name__ == '__main__':
    main()
