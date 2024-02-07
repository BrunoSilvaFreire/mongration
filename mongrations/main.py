import argparse

from mongrations.loading import run_mongration


def main():
    # Create the parser
    parser = argparse.ArgumentParser(description='Mongrate: A tool for managing database migrations.')

    # Add the --mongration argument
    parser.add_argument('--mongration', type=str, help='Path to the mongration script to be executed.', required=True)

    # Parse the command line arguments
    args = parser.parse_args()

    # Call the mongrate function with the provided mongration script path
    run_mongration(args)


if __name__ == '__main__':
    main()
