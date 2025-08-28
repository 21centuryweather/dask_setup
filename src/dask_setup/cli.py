"""Command-line interface for dask_setup profile management."""

from __future__ import annotations

import argparse
import sys
from typing import Any

import yaml

from .config_manager import ConfigManager
from .exceptions import InvalidConfigurationError


def format_profile_list(profiles: dict[str, Any]) -> str:
    """Format profile list for display."""
    if not profiles:
        return "No profiles found."

    lines = []
    builtin_profiles = {name: p for name, p in profiles.items() if p.builtin}
    user_profiles = {name: p for name, p in profiles.items() if not p.builtin}

    if builtin_profiles:
        lines.append(" Built-in Profiles:")
        for name, profile in sorted(builtin_profiles.items()):
            tags_str = f" [{', '.join(profile.tags)}]" if profile.tags else ""
            lines.append(f"  {name:20} - {profile.description}{tags_str}")

    if user_profiles:
        if builtin_profiles:
            lines.append("")
        lines.append(" User Profiles:")
        for name, profile in sorted(user_profiles.items()):
            tags_str = f" [{', '.join(profile.tags)}]" if profile.tags else ""
            lines.append(f"  {name:20} - {profile.description}{tags_str}")

    return "\n".join(lines)


def format_profile_details(profile: Any) -> str:
    """Format profile details for display."""
    config = profile.config

    lines = [
        f"Profile: {profile.name}",
        f"Type: {'Built-in' if profile.builtin else 'User'}",
        f"Description: {profile.description}",
    ]

    if profile.tags:
        lines.append(f"Tags: {', '.join(profile.tags)}")

    lines.extend(
        [
            "",
            "Configuration:",
            f"  Workload Type: {config.workload_type}",
            f"  Max Workers: {config.max_workers or 'auto'}",
            f"  Reserve Memory: {config.reserve_mem_gb} GB",
            f"  Dashboard: {config.dashboard}",
            f"  Adaptive: {config.adaptive}",
        ]
    )

    if config.adaptive and config.min_workers:
        lines.append(f"  Min Workers: {config.min_workers}")

    lines.extend(
        [
            "",
            "Memory Thresholds:",
            f"  Target: {config.memory_target:.0%}",
            f"  Spill: {config.memory_spill:.0%}",
            f"  Pause: {config.memory_pause:.0%}",
            f"  Terminate: {config.memory_terminate:.0%}",
        ]
    )

    if profile.created_at:
        lines.append(f"\nCreated: {profile.created_at}")
    if profile.modified_at:
        lines.append(f"Modified: {profile.modified_at}")

    return "\n".join(lines)


def cmd_list_profiles(args: argparse.Namespace) -> int:
    """List all available profiles."""
    manager = ConfigManager()
    profiles = manager.list_profiles()

    if args.tags:
        # Filter by tags
        tag_set = {tag.strip() for tag in args.tags.split(",")}
        profiles = {
            name: profile
            for name, profile in profiles.items()
            if tag_set.intersection(set(profile.tags))
        }

    print(format_profile_list(profiles))
    return 0


def cmd_show_profile(args: argparse.Namespace) -> int:
    """Show detailed information about a profile."""
    manager = ConfigManager()
    profile = manager.get_profile(args.name)

    if profile is None:
        print(f" Profile '{args.name}' not found.", file=sys.stderr)
        print("Available profiles:")
        profiles = manager.list_profiles()
        print(format_profile_list(profiles))
        return 1

    print(format_profile_details(profile))

    # Show validation results
    is_valid, errors, warnings = manager.validate_profile(args.name)

    if not is_valid:
        print("\n Validation Errors:")
        for error in errors:
            print(f"  - {error}")
    elif warnings:
        print("\n⚠️  Warnings:")
        for warning in warnings:
            print(f"  - {warning}")
    else:
        print("\n Profile is valid")

    return 0 if is_valid else 1


def cmd_create_profile(args: argparse.Namespace) -> int:
    """Create a new profile."""
    manager = ConfigManager()

    # Check if profile already exists
    existing = manager.get_profile(args.name)
    if existing is not None and not args.force:
        print(
            f" Profile '{args.name}' already exists. Use --force to overwrite.",
            file=sys.stderr,
        )
        return 1

    try:
        if args.from_profile:
            # Create from existing profile
            base_profile = manager.get_profile(args.from_profile)
            if base_profile is None:
                print(f" Base profile '{args.from_profile}' not found.", file=sys.stderr)
                return 1

            # Copy configuration and update name
            new_config = base_profile.config
            new_config.name = args.name
            new_config.description = f"Based on {args.from_profile}"

            from .config import ConfigProfile

            profile = ConfigProfile(name=args.name, config=new_config)
        else:
            # Interactive creation
            profile = manager.create_profile_interactively(args.name)

        # Save the profile
        manager.save_profile(profile)
        print(f"\n Profile '{args.name}' created successfully!")

        # Show the created profile
        print("\n" + format_profile_details(profile))

        return 0

    except InvalidConfigurationError as e:
        print(f" Configuration error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f" Failed to create profile: {e}", file=sys.stderr)
        return 1


def cmd_validate_profile(args: argparse.Namespace) -> int:
    """Validate a profile."""
    manager = ConfigManager()

    if args.all:
        # Validate all profiles
        profiles = manager.list_profiles()
        all_valid = True

        for name in sorted(profiles.keys()):
            is_valid, errors, warnings = manager.validate_profile(name)

            status = "Yes" if is_valid else "No"
            print(f"{status} {name}")

            if errors:
                for error in errors:
                    print(f"     Error: {error}")
                all_valid = False

            if warnings:
                for warning in warnings:
                    print(f"     Warning: {warning}")

        return 0 if all_valid else 1

    else:
        # Validate specific profile
        is_valid, errors, warnings = manager.validate_profile(args.name)

        if not manager.get_profile(args.name):
            print(f" Profile '{args.name}' not found.", file=sys.stderr)
            return 1

        if is_valid:
            print(f" Profile '{args.name}' is valid")
        else:
            print(f" Profile '{args.name}' has validation errors:")
            for error in errors:
                print(f"  - {error}")

        if warnings:
            print(" Warnings:")
            for warning in warnings:
                print(f"  - {warning}")

        return 0 if is_valid else 1


def cmd_delete_profile(args: argparse.Namespace) -> int:
    """Delete a user profile."""
    manager = ConfigManager()

    try:
        if manager.delete_profile(args.name):
            print(f" Profile '{args.name}' deleted successfully!")
            return 0
        else:
            print(f" Profile '{args.name}' not found.", file=sys.stderr)
            return 1
    except InvalidConfigurationError as e:
        print(f" {e}", file=sys.stderr)
        return 1


def cmd_export_profile(args: argparse.Namespace) -> int:
    """Export a profile to YAML."""
    manager = ConfigManager()
    profile = manager.get_profile(args.name)

    if profile is None:
        print(f" Profile '{args.name}' not found.", file=sys.stderr)
        return 1

    try:
        yaml_content = yaml.safe_dump(profile.to_dict(), default_flow_style=False, indent=2)

        if args.output:
            with open(args.output, "w") as f:
                f.write(yaml_content)
            print(f" Profile exported to {args.output}")
        else:
            print(yaml_content)

        return 0
    except Exception as e:
        print(f" Failed to export profile: {e}", file=sys.stderr)
        return 1


def create_parser() -> argparse.ArgumentParser:
    """Create the CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog="dask-setup",
        description="Manage dask_setup configuration profiles",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # List profiles
    list_parser = subparsers.add_parser("list", help="List available profiles")
    list_parser.add_argument("--tags", help="Filter by tags (comma-separated)")
    list_parser.set_defaults(func=cmd_list_profiles)

    # Show profile details
    show_parser = subparsers.add_parser("show", help="Show profile details")
    show_parser.add_argument("name", help="Profile name")
    show_parser.set_defaults(func=cmd_show_profile)

    # Create profile
    create_parser = subparsers.add_parser("create", help="Create new profile")
    create_parser.add_argument("name", help="Profile name")
    create_parser.add_argument("--from-profile", help="Create from existing profile")
    create_parser.add_argument("--force", action="store_true", help="Overwrite existing profile")
    create_parser.set_defaults(func=cmd_create_profile)

    # Validate profiles
    validate_parser = subparsers.add_parser("validate", help="Validate profiles")
    validate_group = validate_parser.add_mutually_exclusive_group(required=True)
    validate_group.add_argument("name", nargs="?", help="Profile name to validate")
    validate_group.add_argument("--all", action="store_true", help="Validate all profiles")
    validate_parser.set_defaults(func=cmd_validate_profile)

    # Delete profile
    delete_parser = subparsers.add_parser("delete", help="Delete user profile")
    delete_parser.add_argument("name", help="Profile name")
    delete_parser.set_defaults(func=cmd_delete_profile)

    # Export profile
    export_parser = subparsers.add_parser("export", help="Export profile to YAML")
    export_parser.add_argument("name", help="Profile name")
    export_parser.add_argument("--output", "-o", help="Output file (default: stdout)")
    export_parser.set_defaults(func=cmd_export_profile)

    return parser


def main() -> int:
    """Main CLI entry point."""
    parser = create_parser()
    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        return 1

    try:
        return args.func(args)
    except KeyboardInterrupt:
        print("\n Cancelled by user.", file=sys.stderr)
        return 1
    except Exception as e:
        print(f" Unexpected error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
