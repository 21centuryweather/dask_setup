#!/usr/bin/env python
"""Demonstration of enhanced error handling in dask_setup.

This script shows how the enhanced error handling framework provides
user-friendly error messages with contextual information and actionable
suggestions to help users quickly resolve issues.
"""

import os
import sys

# Add the src directory to the path so we can import dask_setup
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from dask_setup.config import DaskSetupConfig
from dask_setup.error_handling import (
    ConfigurationValidationError,
    DependencyError,
    ErrorContext,
    ResourceConstraintError,
    create_user_friendly_error,
    format_exception_chain,
)

print("🔧 Enhanced Error Handling Demonstration")
print("=" * 50)

# 1. Show environment context detection
print("\n1. 🖥️  Environment Context Detection")
print("-" * 35)
context = ErrorContext()
print(context.get_environment_summary())

# 2. Configuration validation errors
print("\n2. ⚙️  Configuration Validation")
print("-" * 30)
try:
    config = DaskSetupConfig(
        max_workers=-5,  # Invalid: negative
        reserve_mem_gb=1000.0,  # Invalid: too high
        workload_type="invalid",  # Invalid: not a valid type
        dashboard_port=99999,  # Invalid: port too high
    )
except ConfigurationValidationError as e:
    print(str(e))

# 3. Resource constraint errors
print("\n3. 🔋 Resource Constraint Errors")
print("-" * 30)
try:
    raise ResourceConstraintError(resource_type="memory", required=64.0, available=16.0, units="GB")
except ResourceConstraintError as e:
    print(str(e))

# 4. Dependency errors
print("\n4. 📦 Dependency Errors")
print("-" * 20)
try:
    raise DependencyError(missing_package="xarray", feature="xarray chunking recommendations")
except DependencyError as e:
    print(str(e))

# 5. Error factory demonstration
print("\n5. 🏭 Error Factory")
print("-" * 15)
storage_error = create_user_friendly_error(
    "storage", "Could not access Zarr store", storage_path="s3://my-bucket/data.zarr"
)
print(str(storage_error))

# 6. Exception chain formatting
print("\n6. 🔗 Exception Chain Formatting")
print("-" * 30)
original = ValueError("Original error from low-level operation")
enhanced = ConfigurationValidationError(
    field_errors={"test": "caused by original error"},
    invalid_config={"test": "bad_value"},
)
enhanced.__cause__ = original

formatted_chain = format_exception_chain(enhanced)
print(formatted_chain)

print("\n" + "=" * 50)
print("✅ Enhanced Error Handling Demo Complete!")
print("\nKey benefits:")
print("• 🎯 Clear, actionable error messages with emoji")
print("• 🔧 Environment-specific suggestions")
print("• 📋 Comprehensive context information")
print("• 🏥 Diagnostic information for troubleshooting")
print("• 📚 Documentation links for further help")
print("• 🔗 Exception chain formatting for debugging")
