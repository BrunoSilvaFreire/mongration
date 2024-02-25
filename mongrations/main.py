import argparse
import sys
import pathlib



def main():
    # Create the parser
    parser = argparse.ArgumentParser(description='Mongrate: A tool for managing database migrations.')

    # Add the --mongration argument
    parser.add_argument('--mongration', type=str, help='Path to the mongration script to be executed.')
    parser.add_argument('--mongrations_dir', type=str, help='Path to the mongration directory of scripts to be executed.')
    parser.add_argument('--dry-run', action='store_true')

    # Parse the command line arguments
    args = parser.parse_args()
    path = pathlib.Path(__file__)
    main_dir = str(path.parent.parent.absolute())
    if main_dir not in sys.path:
        sys.path.append(main_dir)
    # Call the mongrate function with the provided mongration script path
    from mongrations.program import MongrationProgram
    program = MongrationProgram()
    program.run(args)


if __name__ == '__main__':
    main()
