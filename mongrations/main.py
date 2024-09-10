import argparse
import sys
import pathlib
from mongrations.plan import MongrationStatus
from mongrations.program import MongrationProgram


def main():
    # Create the main parser
    parser = argparse.ArgumentParser(description='Mongrate: A tool for managing MongoDB migrations.')
    parser.add_argument('--url', type=str, help='URL to use to connect to MongoDB.')
   
    subparsers = parser.add_subparsers(dest='command', help='Subcommands')

    # Add the 'run' subcommand (for the existing functionality)
    run_parser = subparsers.add_parser('run', help='Run a migration or a directory of migrations')
    run_parser.add_argument('--mongration', type=str, help='Path to the mongration script to be executed.')
    run_parser.add_argument('--mongrations-dir', type=str, help='Path to the mongration directory of scripts to be executed.')
    run_parser.add_argument('--dry-run', action='store_true')

    # Add the 'manipulate' subcommand (for setting migration states)
    manipulate_parser = subparsers.add_parser('manipulate', help='Manipulate the migration state forcefully')
    manipulate_parser.add_argument('--mongration', type=str, required=True, help='Mongration to manipulate.')
    manipulate_parser.add_argument('--index', type=int, required=False, help='Index to use in case the mongration is not present.')
    manipulate_parser.add_argument('--status', type=str, required=True, choices=[status.name for status in MongrationStatus],
                                   help='Forcefully set the migration status.')

    # Parse the command line arguments
    args = parser.parse_args()
    
    path = pathlib.Path(__file__)
    main_dir = str(path.parent.parent.absolute())
    if main_dir not in sys.path:
        sys.path.append(main_dir)

    # Execute the appropriate function based on the subcommand
    program = MongrationProgram()
    if args.command == 'run':
        # Call the mongrate function with the provided mongration script path
        program.run(args)
    elif args.command == 'manipulate':
        program.manipulate(args)


if __name__ == '__main__':
    main()