import argparse
import sys
import pathlib
from mongrations.plan import MongrationStatus
from mongrations.program import MongrationProgram


def run_mongration_from_args(url, mongration=None, mongrations_dir=None, dry_run=False, 
                             command='run', status=None, index=None):
    """
    Run mongration directly from parameters without using argparse.
    This is the programmatic entry point for tests and direct invocation.
    
    Args:
        url: MongoDB connection URL
        mongration: Path to single mongration script (optional)
        mongrations_dir: Path to directory of mongration scripts (optional)
        dry_run: Whether to run in dry-run mode (default: False)
        command: Command to execute - 'run' or 'manipulate' (default: 'run')
        status: Status to set for 'manipulate' command (optional)
        index: Index to use for 'manipulate' command (optional)
    
    Returns:
        int: Exit code (0 for success, non-zero for failure)
    """
    # Ensure the parent directory is in sys.path
    path = pathlib.Path(__file__)
    main_dir = str(path.parent.parent.absolute())
    if main_dir not in sys.path:
        sys.path.append(main_dir)
    
    # Create an args-like object
    class Args:
        pass
    
    args = Args()
    args.url = url
    args.command = command
    
    program = MongrationProgram()
    
    try:
        if command == 'run':
            args.mongration = mongration
            args.mongrations_dir = mongrations_dir
            args.dry_run = dry_run
            program.run(args)
            return 0
        elif command == 'manipulate':
            if not mongration:
                raise ValueError("--mongration is required for 'manipulate' command")
            if not status:
                raise ValueError("--status is required for 'manipulate' command")
            args.mongration = mongration
            args.status = status
            args.index = index
            program.manipulate(args)
            return 0
        else:
            raise ValueError(f"Unknown command: {command}")
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.exception(f"Error executing mongration: {e}")
        return 1


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
    
    # Use the programmatic entry point
    exit_code = run_mongration_from_args(
        url=args.url,
        mongration=getattr(args, 'mongration', None),
        mongrations_dir=getattr(args, 'mongrations_dir', None),
        dry_run=getattr(args, 'dry_run', False),
        command=args.command,
        status=getattr(args, 'status', None),
        index=getattr(args, 'index', None)
    )
    
    sys.exit(exit_code)


if __name__ == '__main__':
    main()