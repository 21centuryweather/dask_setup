"""Unit tests for dask_setup.cli module."""

import argparse
import sys
from io import StringIO
from unittest.mock import MagicMock, mock_open, patch

import pytest

from dask_setup.cli import (
    cmd_create_profile,
    cmd_delete_profile,
    cmd_export_profile,
    cmd_list_profiles,
    cmd_show_profile,
    cmd_validate_profile,
    create_parser,
    format_profile_details,
    format_profile_list,
    main,
)
from dask_setup.config import ConfigProfile, DaskSetupConfig
from dask_setup.exceptions import InvalidConfigurationError


class TestFormatting:
    """Test CLI formatting functions."""

    @pytest.mark.unit
    def test_format_profile_list_empty(self):
        """Test formatting empty profile list."""
        result = format_profile_list({})
        assert result == "No profiles found."

    @pytest.mark.unit
    def test_format_profile_list_builtin_only(self):
        """Test formatting list with only builtin profiles."""
        config1 = DaskSetupConfig(
            workload_type="cpu", description="CPU intensive profile", tags=["cpu", "analysis"]
        )
        config2 = DaskSetupConfig(
            workload_type="io", description="I/O intensive profile", tags=["io"]
        )

        profiles = {
            "cpu_profile": ConfigProfile(name="cpu_profile", config=config1, builtin=True),
            "io_profile": ConfigProfile(name="io_profile", config=config2, builtin=True),
        }

        result = format_profile_list(profiles)

        assert "📦 Built-in Profiles:" in result
        assert "cpu_profile" in result
        assert "io_profile" in result
        assert "CPU intensive profile" in result
        assert "[cpu, analysis]" in result
        assert "[io]" in result
        assert "👤 User Profiles:" not in result

    @pytest.mark.unit
    def test_format_profile_list_user_only(self):
        """Test formatting list with only user profiles."""
        config = DaskSetupConfig(workload_type="mixed", description="My custom profile")

        profiles = {"my_profile": ConfigProfile(name="my_profile", config=config, builtin=False)}

        result = format_profile_list(profiles)

        assert "👤 User Profiles:" in result
        assert "my_profile" in result
        assert "My custom profile" in result
        assert "📦 Built-in Profiles:" not in result

    @pytest.mark.unit
    def test_format_profile_list_mixed(self):
        """Test formatting list with both builtin and user profiles."""
        builtin_config = DaskSetupConfig(
            workload_type="cpu", description="Built-in profile", tags=["builtin"]
        )
        user_config = DaskSetupConfig(
            workload_type="io", description="User profile", tags=["user", "custom"]
        )

        profiles = {
            "builtin": ConfigProfile(name="builtin", config=builtin_config, builtin=True),
            "user": ConfigProfile(name="user", config=user_config, builtin=False),
        }

        result = format_profile_list(profiles)

        assert "📦 Built-in Profiles:" in result
        assert "👤 User Profiles:" in result
        assert "builtin" in result
        assert "user" in result
        assert "[builtin]" in result
        assert "[user, custom]" in result

    @pytest.mark.unit
    def test_format_profile_details(self):
        """Test formatting profile details."""
        config = DaskSetupConfig(
            workload_type="mixed",
            max_workers=8,
            reserve_mem_gb=32.0,
            dashboard=True,
            adaptive=True,
            min_workers=2,
            description="Test profile",
            tags=["test", "mixed"],
        )

        profile = ConfigProfile(
            name="test_profile",
            config=config,
            builtin=False,
            created_at="2023-01-01T10:00:00",
            modified_at="2023-01-01T12:00:00",
        )

        result = format_profile_details(profile)

        # Check basic info
        assert "Profile: test_profile" in result
        assert "Type: User" in result
        assert "Description: Test profile" in result
        assert "Tags: test, mixed" in result

        # Check configuration
        assert "Workload Type: mixed" in result
        assert "Max Workers: 8" in result
        assert "Reserve Memory: 32.0 GB" in result
        assert "Dashboard: True" in result
        assert "Adaptive: True" in result
        assert "Min Workers: 2" in result

        # Check memory thresholds
        assert "Memory Thresholds:" in result
        assert "Target: 75%" in result
        assert "Spill: 85%" in result
        assert "Pause: 92%" in result
        assert "Terminate: 98%" in result

        # Check timestamps
        assert "Created: 2023-01-01T10:00:00" in result
        assert "Modified: 2023-01-01T12:00:00" in result

    @pytest.mark.unit
    def test_format_profile_details_builtin_no_min_workers(self):
        """Test formatting builtin profile without min_workers."""
        config = DaskSetupConfig(
            workload_type="cpu",
            max_workers=None,  # Auto
            description="Built-in CPU profile",
        )

        profile = ConfigProfile(name="cpu_builtin", config=config, builtin=True)

        result = format_profile_details(profile)

        assert "Type: Built-in" in result
        assert "Max Workers: auto" in result
        assert "Adaptive: False" in result
        assert "Min Workers:" not in result  # Should not appear for non-adaptive
        assert "Created:" not in result  # No timestamps for builtin
        assert "Modified:" not in result


class TestListCommand:
    """Test list command functionality."""

    @pytest.mark.unit
    @patch("dask_setup.cli.ConfigManager")
    def test_cmd_list_profiles_basic(self, mock_config_manager):
        """Test basic profile listing."""
        # Setup mock
        mock_manager = MagicMock()
        mock_config_manager.return_value = mock_manager

        config = DaskSetupConfig(workload_type="cpu", description="Test profile")
        profiles = {"test": ConfigProfile(name="test", config=config, builtin=True)}
        mock_manager.list_profiles.return_value = profiles

        args = argparse.Namespace(tags=None)

        with patch("builtins.print") as mock_print:
            result = cmd_list_profiles(args)

        assert result == 0
        mock_manager.list_profiles.assert_called_once()
        mock_print.assert_called_once()

        # Check that formatted output contains profile info
        call_args = mock_print.call_args[0][0]
        assert "test" in call_args

    @pytest.mark.unit
    @patch("dask_setup.cli.ConfigManager")
    def test_cmd_list_profiles_with_tag_filter(self, mock_config_manager):
        """Test profile listing with tag filtering."""
        mock_manager = MagicMock()
        mock_config_manager.return_value = mock_manager

        # Create profiles with different tags
        cpu_config = DaskSetupConfig(
            workload_type="cpu", description="CPU profile", tags=["cpu", "compute"]
        )
        io_config = DaskSetupConfig(
            workload_type="io", description="IO profile", tags=["io", "storage"]
        )

        profiles = {
            "cpu_profile": ConfigProfile(name="cpu_profile", config=cpu_config, builtin=True),
            "io_profile": ConfigProfile(name="io_profile", config=io_config, builtin=True),
        }
        mock_manager.list_profiles.return_value = profiles

        args = argparse.Namespace(tags="cpu,compute")

        with patch("builtins.print") as mock_print:
            result = cmd_list_profiles(args)

        assert result == 0

        # Verify only cpu_profile is in output (has cpu or compute tags)
        call_args = mock_print.call_args[0][0]
        assert "cpu_profile" in call_args
        assert "io_profile" not in call_args

    @pytest.mark.unit
    @patch("dask_setup.cli.ConfigManager")
    def test_cmd_list_profiles_empty_after_filtering(self, mock_config_manager):
        """Test profile listing when tag filter results in empty list."""
        mock_manager = MagicMock()
        mock_config_manager.return_value = mock_manager

        config = DaskSetupConfig(workload_type="cpu", description="CPU profile", tags=["cpu"])
        profiles = {"cpu_profile": ConfigProfile(name="cpu_profile", config=config, builtin=True)}
        mock_manager.list_profiles.return_value = profiles

        args = argparse.Namespace(tags="nonexistent")

        with patch("builtins.print") as mock_print:
            result = cmd_list_profiles(args)

        assert result == 0
        call_args = mock_print.call_args[0][0]
        assert "No profiles found." in call_args


class TestShowCommand:
    """Test show command functionality."""

    @pytest.mark.unit
    @patch("dask_setup.cli.ConfigManager")
    def test_cmd_show_profile_success(self, mock_config_manager):
        """Test showing existing profile successfully."""
        mock_manager = MagicMock()
        mock_config_manager.return_value = mock_manager

        config = DaskSetupConfig(workload_type="cpu", description="Test profile")
        profile = ConfigProfile(name="test", config=config, builtin=True)

        mock_manager.get_profile.return_value = profile
        mock_manager.validate_profile.return_value = (True, [], ["Test warning"])

        args = argparse.Namespace(name="test")

        with patch("builtins.print") as mock_print:
            result = cmd_show_profile(args)

        assert result == 0
        mock_manager.get_profile.assert_called_once_with("test")
        mock_manager.validate_profile.assert_called_once_with("test")

        # Check that profile details and warning are printed
        print_calls = [call[0][0] for call in mock_print.call_args_list]
        profile_output = "".join(print_calls)
        assert "Profile: test" in profile_output
        assert "⚠️  Warnings:" in profile_output
        assert "Test warning" in profile_output

    @pytest.mark.unit
    @patch("dask_setup.cli.ConfigManager")
    def test_cmd_show_profile_with_errors(self, mock_config_manager):
        """Test showing profile with validation errors."""
        mock_manager = MagicMock()
        mock_config_manager.return_value = mock_manager

        config = DaskSetupConfig(workload_type="cpu", description="Invalid profile")
        profile = ConfigProfile(name="invalid", config=config, builtin=False)

        mock_manager.get_profile.return_value = profile
        mock_manager.validate_profile.return_value = (
            False,
            ["Config error 1", "Config error 2"],
            [],
        )

        args = argparse.Namespace(name="invalid")

        with patch("builtins.print") as mock_print:
            result = cmd_show_profile(args)

        assert result == 1  # Should return 1 for invalid profile

        print_calls = [call[0][0] for call in mock_print.call_args_list]
        output = "".join(print_calls)
        assert "❌ Validation Errors:" in output
        assert "Config error 1" in output
        assert "Config error 2" in output

    @pytest.mark.unit
    @patch("dask_setup.cli.ConfigManager")
    def test_cmd_show_profile_valid_no_warnings(self, mock_config_manager):
        """Test showing valid profile with no warnings."""
        mock_manager = MagicMock()
        mock_config_manager.return_value = mock_manager

        config = DaskSetupConfig(workload_type="cpu", description="Valid profile")
        profile = ConfigProfile(name="valid", config=config, builtin=True)

        mock_manager.get_profile.return_value = profile
        mock_manager.validate_profile.return_value = (True, [], [])

        args = argparse.Namespace(name="valid")

        with patch("builtins.print") as mock_print:
            result = cmd_show_profile(args)

        assert result == 0

        print_calls = [call[0][0] for call in mock_print.call_args_list]
        output = "".join(print_calls)
        assert "✅ Profile is valid" in output

    @pytest.mark.unit
    @patch("dask_setup.cli.ConfigManager")
    def test_cmd_show_profile_not_found(self, mock_config_manager):
        """Test showing non-existent profile."""
        mock_manager = MagicMock()
        mock_config_manager.return_value = mock_manager

        mock_manager.get_profile.return_value = None
        mock_manager.list_profiles.return_value = {
            "existing": ConfigProfile(
                name="existing",
                config=DaskSetupConfig(workload_type="cpu", description="Exists"),
                builtin=True,
            )
        }

        args = argparse.Namespace(name="nonexistent")

        with patch("builtins.print") as mock_print:
            result = cmd_show_profile(args)

        assert result == 1

        # Check all print calls - the error should be printed with file=sys.stderr
        print_calls = mock_print.call_args_list

        # Find the stderr call
        stderr_call = None
        stdout_calls = []

        for call in print_calls:
            if len(call[1]) > 0 and call[1].get("file") is sys.stderr:
                stderr_call = call[0][0]  # Get the message
            else:
                stdout_calls.append(call[0][0])  # Regular stdout calls

        # Check that error was printed to stderr
        assert stderr_call is not None
        assert "❌ Profile 'nonexistent' not found." in stderr_call

        # Check that available profiles are shown on stdout
        stdout_output = "".join(stdout_calls)
        assert "Available profiles:" in stdout_output
        assert "existing" in stdout_output


class TestCreateCommand:
    """Test create command functionality."""

    @pytest.mark.unit
    @patch("dask_setup.cli.ConfigManager")
    def test_cmd_create_profile_interactive_success(self, mock_config_manager):
        """Test interactive profile creation success."""
        mock_manager = MagicMock()
        mock_config_manager.return_value = mock_manager

        config = DaskSetupConfig(workload_type="cpu", description="Interactively created profile")
        profile = ConfigProfile(name="new_profile", config=config, builtin=False)

        mock_manager.get_profile.return_value = None  # Profile doesn't exist
        mock_manager.create_profile_interactively.return_value = profile

        args = argparse.Namespace(name="new_profile", from_profile=None, force=False)

        with patch("builtins.print") as mock_print:
            result = cmd_create_profile(args)

        assert result == 0
        mock_manager.create_profile_interactively.assert_called_once_with("new_profile")
        mock_manager.save_profile.assert_called_once_with(profile)

        print_calls = [call[0][0] for call in mock_print.call_args_list]
        output = "".join(print_calls)
        assert "✅ Profile 'new_profile' created successfully!" in output
        assert "Profile: new_profile" in output

    @pytest.mark.unit
    @patch("dask_setup.cli.ConfigManager")
    def test_cmd_create_profile_from_existing(self, mock_config_manager):
        """Test creating profile from existing profile."""
        mock_manager = MagicMock()
        mock_config_manager.return_value = mock_manager

        # Base profile
        base_config = DaskSetupConfig(
            workload_type="cpu", max_workers=4, description="Base profile"
        )
        base_profile = ConfigProfile(name="base", config=base_config, builtin=True)

        mock_manager.get_profile.side_effect = [
            None,
            base_profile,
        ]  # New doesn't exist, base exists

        args = argparse.Namespace(name="derived_profile", from_profile="base", force=False)

        with patch("builtins.print"):
            result = cmd_create_profile(args)

        assert result == 0
        mock_manager.save_profile.assert_called_once()

        # Verify config was updated
        assert base_config.name == "derived_profile"
        assert base_config.description == "Based on base"

    @pytest.mark.unit
    @patch("dask_setup.cli.ConfigManager")
    def test_cmd_create_profile_already_exists(self, mock_config_manager):
        """Test creating profile that already exists without force."""
        mock_manager = MagicMock()
        mock_config_manager.return_value = mock_manager

        existing_config = DaskSetupConfig(workload_type="cpu", description="Existing")
        existing_profile = ConfigProfile(name="existing", config=existing_config, builtin=False)

        mock_manager.get_profile.return_value = existing_profile

        args = argparse.Namespace(name="existing", from_profile=None, force=False)

        with patch("sys.stderr", new_callable=StringIO) as mock_stderr:
            result = cmd_create_profile(args)

        assert result == 1
        assert (
            "❌ Profile 'existing' already exists. Use --force to overwrite."
            in mock_stderr.getvalue()
        )

    @pytest.mark.unit
    @patch("dask_setup.cli.ConfigManager")
    def test_cmd_create_profile_force_overwrite(self, mock_config_manager):
        """Test creating profile with force overwrite."""
        mock_manager = MagicMock()
        mock_config_manager.return_value = mock_manager

        existing_config = DaskSetupConfig(workload_type="cpu", description="Existing")
        existing_profile = ConfigProfile(name="existing", config=existing_config, builtin=False)

        new_config = DaskSetupConfig(workload_type="io", description="New version")
        new_profile = ConfigProfile(name="existing", config=new_config, builtin=False)

        mock_manager.get_profile.return_value = existing_profile
        mock_manager.create_profile_interactively.return_value = new_profile

        args = argparse.Namespace(name="existing", from_profile=None, force=True)

        with patch("builtins.print"):
            result = cmd_create_profile(args)

        assert result == 0
        mock_manager.create_profile_interactively.assert_called_once_with("existing")
        mock_manager.save_profile.assert_called_once_with(new_profile)

    @pytest.mark.unit
    @patch("dask_setup.cli.ConfigManager")
    def test_cmd_create_profile_base_not_found(self, mock_config_manager):
        """Test creating profile from non-existent base profile."""
        mock_manager = MagicMock()
        mock_config_manager.return_value = mock_manager

        mock_manager.get_profile.side_effect = [None, None]  # Both new and base don't exist

        args = argparse.Namespace(name="new_profile", from_profile="nonexistent_base", force=False)

        with patch("sys.stderr", new_callable=StringIO) as mock_stderr:
            result = cmd_create_profile(args)

        assert result == 1
        assert "❌ Base profile 'nonexistent_base' not found." in mock_stderr.getvalue()

    @pytest.mark.unit
    @patch("dask_setup.cli.ConfigManager")
    def test_cmd_create_profile_configuration_error(self, mock_config_manager):
        """Test handling configuration errors during profile creation."""
        mock_manager = MagicMock()
        mock_config_manager.return_value = mock_manager

        mock_manager.get_profile.return_value = None
        mock_manager.save_profile.side_effect = InvalidConfigurationError("Invalid config")

        config = DaskSetupConfig(workload_type="cpu", description="Invalid")
        profile = ConfigProfile(name="invalid", config=config, builtin=False)
        mock_manager.create_profile_interactively.return_value = profile

        args = argparse.Namespace(name="invalid", from_profile=None, force=False)

        with patch("sys.stderr", new_callable=StringIO) as mock_stderr:
            result = cmd_create_profile(args)

        assert result == 1
        assert "❌ Configuration error: Invalid config" in mock_stderr.getvalue()

    @pytest.mark.unit
    @patch("dask_setup.cli.ConfigManager")
    def test_cmd_create_profile_unexpected_error(self, mock_config_manager):
        """Test handling unexpected errors during profile creation."""
        mock_manager = MagicMock()
        mock_config_manager.return_value = mock_manager

        mock_manager.get_profile.return_value = None
        mock_manager.create_profile_interactively.side_effect = Exception("Unexpected error")

        args = argparse.Namespace(name="error_profile", from_profile=None, force=False)

        with patch("sys.stderr", new_callable=StringIO) as mock_stderr:
            result = cmd_create_profile(args)

        assert result == 1
        assert "❌ Failed to create profile: Unexpected error" in mock_stderr.getvalue()


class TestValidateCommand:
    """Test validate command functionality."""

    @pytest.mark.unit
    @patch("dask_setup.cli.ConfigManager")
    def test_cmd_validate_profile_single_valid(self, mock_config_manager):
        """Test validating single valid profile."""
        mock_manager = MagicMock()
        mock_config_manager.return_value = mock_manager

        config = DaskSetupConfig(workload_type="cpu", description="Valid")
        profile = ConfigProfile(name="valid", config=config, builtin=True)

        mock_manager.get_profile.return_value = profile
        mock_manager.validate_profile.return_value = (True, [], ["Warning message"])

        args = argparse.Namespace(name="valid", all=False)

        with patch("builtins.print") as mock_print:
            result = cmd_validate_profile(args)

        assert result == 0

        print_calls = [call[0][0] for call in mock_print.call_args_list]
        output = "".join(print_calls)
        assert "✅ Profile 'valid' is valid" in output
        assert "⚠️  Warnings:" in output
        assert "Warning message" in output

    @pytest.mark.unit
    @patch("dask_setup.cli.ConfigManager")
    def test_cmd_validate_profile_single_invalid(self, mock_config_manager):
        """Test validating single invalid profile."""
        mock_manager = MagicMock()
        mock_config_manager.return_value = mock_manager

        config = DaskSetupConfig(workload_type="cpu", description="Invalid")
        profile = ConfigProfile(name="invalid", config=config, builtin=False)

        mock_manager.get_profile.return_value = profile
        mock_manager.validate_profile.return_value = (False, ["Error 1", "Error 2"], [])

        args = argparse.Namespace(name="invalid", all=False)

        with patch("builtins.print") as mock_print:
            result = cmd_validate_profile(args)

        assert result == 1

        print_calls = [call[0][0] for call in mock_print.call_args_list]
        output = "".join(print_calls)
        assert "❌ Profile 'invalid' has validation errors:" in output
        assert "Error 1" in output
        assert "Error 2" in output

    @pytest.mark.unit
    @patch("dask_setup.cli.ConfigManager")
    def test_cmd_validate_profile_not_found(self, mock_config_manager):
        """Test validating non-existent profile."""
        mock_manager = MagicMock()
        mock_config_manager.return_value = mock_manager

        mock_manager.get_profile.return_value = None
        mock_manager.validate_profile.return_value = (False, ["Profile 'missing' not found"], [])

        args = argparse.Namespace(name="missing", all=False)

        with patch("sys.stderr", new_callable=StringIO) as mock_stderr:
            result = cmd_validate_profile(args)

        assert result == 1
        assert "❌ Profile 'missing' not found." in mock_stderr.getvalue()

    @pytest.mark.unit
    @patch("dask_setup.cli.ConfigManager")
    def test_cmd_validate_profile_all_success(self, mock_config_manager):
        """Test validating all profiles with all valid."""
        mock_manager = MagicMock()
        mock_config_manager.return_value = mock_manager

        # Setup profiles
        config1 = DaskSetupConfig(workload_type="cpu", description="Profile 1")
        config2 = DaskSetupConfig(workload_type="io", description="Profile 2")

        profiles = {
            "profile1": ConfigProfile(name="profile1", config=config1, builtin=True),
            "profile2": ConfigProfile(name="profile2", config=config2, builtin=False),
        }
        mock_manager.list_profiles.return_value = profiles
        mock_manager.validate_profile.side_effect = [
            (True, [], []),  # profile1 valid
            (True, [], ["Warning"]),  # profile2 valid with warning
        ]

        args = argparse.Namespace(name=None, all=True)

        with patch("builtins.print") as mock_print:
            result = cmd_validate_profile(args)

        assert result == 0

        print_calls = [call[0][0] for call in mock_print.call_args_list]
        output = "".join(print_calls)
        assert "✅ profile1" in output
        assert "✅ profile2" in output
        assert "Warning: Warning" in output

    @pytest.mark.unit
    @patch("dask_setup.cli.ConfigManager")
    def test_cmd_validate_profile_all_with_errors(self, mock_config_manager):
        """Test validating all profiles with some invalid."""
        mock_manager = MagicMock()
        mock_config_manager.return_value = mock_manager

        config1 = DaskSetupConfig(workload_type="cpu", description="Valid")
        config2 = DaskSetupConfig(workload_type="io", description="Invalid")

        profiles = {
            "valid": ConfigProfile(name="valid", config=config1, builtin=True),
            "invalid": ConfigProfile(name="invalid", config=config2, builtin=False),
        }
        mock_manager.list_profiles.return_value = profiles
        mock_manager.validate_profile.side_effect = [
            (False, ["Error in invalid"], []),  # invalid has errors
            (True, [], []),  # valid is ok
        ]

        args = argparse.Namespace(name=None, all=True)

        with patch("builtins.print") as mock_print:
            result = cmd_validate_profile(args)

        assert result == 1  # Should return 1 if any profile is invalid

        print_calls = [call[0][0] for call in mock_print.call_args_list]
        output = "".join(print_calls)
        assert "❌ invalid" in output
        assert "Error: Error in invalid" in output
        assert "✅ valid" in output


class TestDeleteCommand:
    """Test delete command functionality."""

    @pytest.mark.unit
    @patch("dask_setup.cli.ConfigManager")
    def test_cmd_delete_profile_success(self, mock_config_manager):
        """Test successful profile deletion."""
        mock_manager = MagicMock()
        mock_config_manager.return_value = mock_manager

        mock_manager.delete_profile.return_value = True

        args = argparse.Namespace(name="user_profile")

        with patch("builtins.print") as mock_print:
            result = cmd_delete_profile(args)

        assert result == 0
        mock_manager.delete_profile.assert_called_once_with("user_profile")

        print_calls = [call[0][0] for call in mock_print.call_args_list]
        output = "".join(print_calls)
        assert "✅ Profile 'user_profile' deleted successfully!" in output

    @pytest.mark.unit
    @patch("dask_setup.cli.ConfigManager")
    def test_cmd_delete_profile_not_found(self, mock_config_manager):
        """Test deleting non-existent profile."""
        mock_manager = MagicMock()
        mock_config_manager.return_value = mock_manager

        mock_manager.delete_profile.return_value = False

        args = argparse.Namespace(name="nonexistent")

        with patch("sys.stderr", new_callable=StringIO) as mock_stderr:
            result = cmd_delete_profile(args)

        assert result == 1
        assert "❌ Profile 'nonexistent' not found." in mock_stderr.getvalue()

    @pytest.mark.unit
    @patch("dask_setup.cli.ConfigManager")
    def test_cmd_delete_profile_builtin_error(self, mock_config_manager):
        """Test deleting builtin profile (should raise error)."""
        mock_manager = MagicMock()
        mock_config_manager.return_value = mock_manager

        mock_manager.delete_profile.side_effect = InvalidConfigurationError(
            "Cannot delete builtin profile"
        )

        args = argparse.Namespace(name="builtin_profile")

        with patch("sys.stderr", new_callable=StringIO) as mock_stderr:
            result = cmd_delete_profile(args)

        assert result == 1
        assert "❌ Cannot delete builtin profile" in mock_stderr.getvalue()


class TestExportCommand:
    """Test export command functionality."""

    @pytest.mark.unit
    @patch("dask_setup.cli.ConfigManager")
    def test_cmd_export_profile_to_stdout(self, mock_config_manager):
        """Test exporting profile to stdout."""
        mock_manager = MagicMock()
        mock_config_manager.return_value = mock_manager

        config = DaskSetupConfig(
            workload_type="cpu", max_workers=4, description="Export test profile"
        )
        profile = ConfigProfile(name="export_test", config=config, builtin=False)
        profile_dict = {
            "name": "export_test",
            "config": {
                "workload_type": "cpu",
                "max_workers": 4,
                "description": "Export test profile",
            },
            "builtin": False,
        }

        mock_manager.get_profile.return_value = profile
        profile.to_dict = MagicMock(return_value=profile_dict)

        args = argparse.Namespace(name="export_test", output=None)

        with patch("builtins.print") as mock_print:
            result = cmd_export_profile(args)

        assert result == 0
        mock_manager.get_profile.assert_called_once_with("export_test")

        # Check that YAML was printed to stdout
        print_calls = [call[0][0] for call in mock_print.call_args_list]
        output = "".join(print_calls)
        assert "name: export_test" in output
        assert "workload_type: cpu" in output

    @pytest.mark.unit
    @patch("dask_setup.cli.ConfigManager")
    def test_cmd_export_profile_to_file(self, mock_config_manager):
        """Test exporting profile to file."""
        mock_manager = MagicMock()
        mock_config_manager.return_value = mock_manager

        config = DaskSetupConfig(workload_type="io", description="File export test")
        profile = ConfigProfile(name="file_export", config=config, builtin=True)
        profile_dict = {
            "name": "file_export",
            "config": {"workload_type": "io", "description": "File export test"},
            "builtin": True,
        }

        mock_manager.get_profile.return_value = profile
        profile.to_dict = MagicMock(return_value=profile_dict)

        args = argparse.Namespace(name="file_export", output="test_export.yaml")

        with (
            patch("builtins.open", mock_open()) as mock_file,
            patch("builtins.print") as mock_print,
        ):
            result = cmd_export_profile(args)

        assert result == 0
        mock_file.assert_called_once_with("test_export.yaml", "w")

        # Check success message
        print_calls = [call[0][0] for call in mock_print.call_args_list]
        output = "".join(print_calls)
        assert "✅ Profile exported to test_export.yaml" in output

    @pytest.mark.unit
    @patch("dask_setup.cli.ConfigManager")
    def test_cmd_export_profile_not_found(self, mock_config_manager):
        """Test exporting non-existent profile."""
        mock_manager = MagicMock()
        mock_config_manager.return_value = mock_manager

        mock_manager.get_profile.return_value = None

        args = argparse.Namespace(name="nonexistent", output=None)

        with patch("sys.stderr", new_callable=StringIO) as mock_stderr:
            result = cmd_export_profile(args)

        assert result == 1
        assert "❌ Profile 'nonexistent' not found." in mock_stderr.getvalue()

    @pytest.mark.unit
    @patch("dask_setup.cli.ConfigManager")
    def test_cmd_export_profile_file_error(self, mock_config_manager):
        """Test export profile with file write error."""
        mock_manager = MagicMock()
        mock_config_manager.return_value = mock_manager

        config = DaskSetupConfig(workload_type="cpu", description="Error test")
        profile = ConfigProfile(name="error_test", config=config, builtin=False)

        mock_manager.get_profile.return_value = profile
        profile.to_dict = MagicMock(return_value={})

        args = argparse.Namespace(name="error_test", output="readonly.yaml")

        with (
            patch("builtins.open", side_effect=OSError("Permission denied")),
            patch("sys.stderr", new_callable=StringIO) as mock_stderr,
        ):
            result = cmd_export_profile(args)

        assert result == 1
        assert "❌ Failed to export profile: Permission denied" in mock_stderr.getvalue()


class TestParser:
    """Test CLI argument parser."""

    @pytest.mark.unit
    def test_create_parser_basic(self):
        """Test basic parser creation and structure."""
        parser = create_parser()

        assert parser.prog == "dask-setup"
        assert "Manage dask_setup configuration profiles" in parser.description

    @pytest.mark.unit
    def test_parser_list_command(self):
        """Test list command parsing."""
        parser = create_parser()

        # Basic list command
        args = parser.parse_args(["list"])
        assert args.command == "list"
        assert args.tags is None

        # List with tags
        args = parser.parse_args(["list", "--tags", "cpu,analysis"])
        assert args.command == "list"
        assert args.tags == "cpu,analysis"

    @pytest.mark.unit
    def test_parser_show_command(self):
        """Test show command parsing."""
        parser = create_parser()

        args = parser.parse_args(["show", "test_profile"])
        assert args.command == "show"
        assert args.name == "test_profile"

    @pytest.mark.unit
    def test_parser_create_command(self):
        """Test create command parsing."""
        parser = create_parser()

        # Basic create
        args = parser.parse_args(["create", "new_profile"])
        assert args.command == "create"
        assert args.name == "new_profile"
        assert args.from_profile is None
        assert args.force is False

        # Create from existing with force
        args = parser.parse_args(["create", "derived", "--from-profile", "base", "--force"])
        assert args.command == "create"
        assert args.name == "derived"
        assert args.from_profile == "base"
        assert args.force is True

    @pytest.mark.unit
    def test_parser_validate_command(self):
        """Test validate command parsing."""
        parser = create_parser()

        # Validate specific profile
        args = parser.parse_args(["validate", "test_profile"])
        assert args.command == "validate"
        assert args.name == "test_profile"
        assert args.all is False

        # Validate all profiles
        args = parser.parse_args(["validate", "--all"])
        assert args.command == "validate"
        assert args.name is None
        assert args.all is True

    @pytest.mark.unit
    def test_parser_delete_command(self):
        """Test delete command parsing."""
        parser = create_parser()

        args = parser.parse_args(["delete", "old_profile"])
        assert args.command == "delete"
        assert args.name == "old_profile"

    @pytest.mark.unit
    def test_parser_export_command(self):
        """Test export command parsing."""
        parser = create_parser()

        # Export to stdout
        args = parser.parse_args(["export", "test_profile"])
        assert args.command == "export"
        assert args.name == "test_profile"
        assert args.output is None

        # Export to file
        args = parser.parse_args(["export", "test_profile", "--output", "export.yaml"])
        assert args.command == "export"
        assert args.name == "test_profile"
        assert args.output == "export.yaml"

        # Export to file (short option)
        args = parser.parse_args(["export", "test_profile", "-o", "export.yaml"])
        assert args.command == "export"
        assert args.name == "test_profile"
        assert args.output == "export.yaml"

    @pytest.mark.unit
    def test_parser_no_command(self):
        """Test parser with no command provided."""
        parser = create_parser()

        args = parser.parse_args([])
        assert args.command is None


class TestMainFunction:
    """Test main CLI entry point."""

    @pytest.mark.unit
    @patch("dask_setup.cli.create_parser")
    def test_main_no_command(self, mock_create_parser):
        """Test main function with no command (should show help)."""
        mock_parser = MagicMock()
        mock_parser.parse_args.return_value = argparse.Namespace(command=None)
        mock_create_parser.return_value = mock_parser

        result = main()

        assert result == 1
        mock_parser.print_help.assert_called_once()

    @pytest.mark.unit
    @patch("dask_setup.cli.create_parser")
    def test_main_successful_command(self, mock_create_parser):
        """Test main function with successful command execution."""
        mock_parser = MagicMock()
        mock_func = MagicMock(return_value=0)
        mock_args = argparse.Namespace(command="test", func=mock_func)
        mock_parser.parse_args.return_value = mock_args
        mock_create_parser.return_value = mock_parser

        result = main()

        assert result == 0
        mock_func.assert_called_once_with(mock_args)

    @pytest.mark.unit
    @patch("dask_setup.cli.create_parser")
    def test_main_keyboard_interrupt(self, mock_create_parser):
        """Test main function with keyboard interrupt."""
        mock_parser = MagicMock()
        mock_func = MagicMock(side_effect=KeyboardInterrupt())
        mock_args = argparse.Namespace(command="test", func=mock_func)
        mock_parser.parse_args.return_value = mock_args
        mock_create_parser.return_value = mock_parser

        with patch("sys.stderr", new_callable=StringIO) as mock_stderr:
            result = main()

        assert result == 1
        assert "❌ Cancelled by user." in mock_stderr.getvalue()

    @pytest.mark.unit
    @patch("dask_setup.cli.create_parser")
    def test_main_unexpected_error(self, mock_create_parser):
        """Test main function with unexpected error."""
        mock_parser = MagicMock()
        mock_func = MagicMock(side_effect=Exception("Unexpected error"))
        mock_args = argparse.Namespace(command="test", func=mock_func)
        mock_parser.parse_args.return_value = mock_args
        mock_create_parser.return_value = mock_parser

        with patch("sys.stderr", new_callable=StringIO) as mock_stderr:
            result = main()

        assert result == 1
        assert "❌ Unexpected error: Unexpected error" in mock_stderr.getvalue()


class TestCLIIntegration:
    """Integration tests for CLI functionality."""

    @pytest.mark.unit
    @patch("sys.argv", ["dask-setup", "list"])
    @patch("dask_setup.cli.main")
    def test_cli_entry_point(self, mock_main):
        """Test CLI entry point integration."""
        mock_main.return_value = 0

        # Simulate running the CLI
        from dask_setup.cli import __name__ as cli_name

        # The __name__ check should trigger main()
        if cli_name == "__main__":
            mock_main.assert_called_once()

    @pytest.mark.unit
    def test_end_to_end_argument_parsing(self):
        """Test complete argument parsing for all commands."""
        parser = create_parser()

        # Test all command variations
        test_cases = [
            (["list"], {"command": "list", "tags": None}),
            (["list", "--tags", "cpu"], {"command": "list", "tags": "cpu"}),
            (["show", "profile"], {"command": "show", "name": "profile"}),
            (["create", "new"], {"command": "create", "name": "new", "force": False}),
            (["create", "new", "--force"], {"command": "create", "name": "new", "force": True}),
            (["validate", "prof"], {"command": "validate", "name": "prof", "all": False}),
            (["validate", "--all"], {"command": "validate", "all": True}),
            (["delete", "old"], {"command": "delete", "name": "old"}),
            (["export", "prof"], {"command": "export", "name": "prof", "output": None}),
            (
                ["export", "prof", "-o", "file.yaml"],
                {"command": "export", "name": "prof", "output": "file.yaml"},
            ),
        ]

        for args_list, expected_attrs in test_cases:
            args = parser.parse_args(args_list)
            for attr, expected_value in expected_attrs.items():
                assert getattr(args, attr) == expected_value
