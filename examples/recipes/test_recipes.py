#!/usr/bin/env python
"""Test script for dask_setup recipes.

This script performs basic tests to verify that the recipe environment
is properly configured and that core functionality works as expected.
"""

import importlib
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple

# Add parent directories to path for imports
parent_dir = Path(__file__).parent
src_dir = parent_dir.parent.parent / "src"
sys.path.insert(0, str(parent_dir))
sys.path.insert(0, str(src_dir))


def test_imports() -> Tuple[bool, List[str]]:
    """Test that all critical imports work."""
    print("🧪 Testing imports...")
    
    critical_imports = [
        'dask',
        'distributed', 
        'xarray',
        'numpy',
        'pandas'
    ]
    
    optional_imports = [
        'matplotlib',
        'scipy',
        'zarr',
        'rechunker',
        'psutil',
        'netcdf4',
        'h5netcdf'
    ]
    
    failures = []
    
    # Test critical imports
    print("\n📦 Critical imports:")
    for module_name in critical_imports:
        try:
            importlib.import_module(module_name)
            print(f"   ✅ {module_name}")
        except ImportError as e:
            print(f"   ❌ {module_name}: {e}")
            failures.append(f"Critical: {module_name}")
    
    # Test optional imports
    print("\n📦 Optional imports:")
    for module_name in optional_imports:
        try:
            importlib.import_module(module_name)
            print(f"   ✅ {module_name}")
        except ImportError:
            print(f"   ⚠️  {module_name} (optional)")
    
    # Test dask_setup specifically
    print("\n📦 dask_setup:")
    try:
        import dask_setup
        print(f"   ✅ dask_setup")
    except ImportError as e:
        print(f"   ❌ dask_setup: {e}")
        failures.append("Critical: dask_setup")
    
    return len(failures) == 0, failures


def test_basic_dask_functionality() -> bool:
    """Test basic dask functionality."""
    print("\n🧪 Testing basic dask functionality...")
    
    try:
        import dask.array as da
        import numpy as np
        
        # Create a simple dask array
        x = da.random.random((100, 100), chunks=(50, 50))
        result = x.mean().compute()
        
        print(f"   ✅ Dask array computation: mean = {result:.6f}")
        return True
        
    except Exception as e:
        print(f"   ❌ Dask array test failed: {e}")
        return False


def test_dask_setup_basic() -> bool:
    """Test basic dask_setup functionality."""
    print("\n🧪 Testing dask_setup basic functionality...")
    
    try:
        import dask_setup
        
        # Test basic client creation
        client, cluster, temp_dir = dask_setup.setup_dask_client(
            workload_type="cpu",
            max_workers=1,
            reserve_mem_gb=2.0,
            dashboard=False
        )
        
        # Simple computation test
        import dask.array as da
        x = da.ones((10, 10), chunks=(5, 5))
        result = x.sum().compute()
        
        # Clean up
        client.close()
        cluster.close()
        
        print(f"   ✅ dask_setup client creation and computation: sum = {result}")
        return True
        
    except Exception as e:
        print(f"   ❌ dask_setup test failed: {e}")
        return False


def test_utils_module() -> bool:
    """Test utils module functionality."""
    print("\n🧪 Testing utils module...")
    
    try:
        from utils import format_bytes, format_duration, timer
        
        # Test format_bytes
        formatted = format_bytes(1024*1024*1024)
        print(f"   ✅ format_bytes: {formatted}")
        
        # Test format_duration
        duration = format_duration(3661.5)
        print(f"   ✅ format_duration: {duration}")
        
        # Test timer
        with timer("test operation") as t:
            time.sleep(0.1)
        
        print(f"   ✅ timer: {t['total']:.2f}s")
        return True
        
    except Exception as e:
        print(f"   ❌ utils module test failed: {e}")
        return False


def test_sample_recipe() -> bool:
    """Test that we can import and inspect a sample recipe."""
    print("\n🧪 Testing sample recipe import...")
    
    try:
        # Try to import from basic usage recipe
        basic_dir = parent_dir / "01_basic"
        sys.path.insert(0, str(basic_dir))
        
        # Import the basic usage module
        spec = importlib.util.spec_from_file_location(
            "basic_usage", 
            basic_dir / "basic_usage.py"
        )
        basic_module = importlib.util.module_from_spec(spec)
        
        # Just test that it loads without errors
        spec.loader.exec_module(basic_module)
        
        print("   ✅ Basic recipe import successful")
        return True
        
    except Exception as e:
        print(f"   ❌ Sample recipe import failed: {e}")
        return False


def run_all_tests() -> int:
    """Run all tests and return exit code."""
    print("🧪 DASK_SETUP RECIPES TEST SUITE")
    print("=" * 60)
    
    tests = [
        ("Import Tests", test_imports),
        ("Dask Functionality", test_basic_dask_functionality), 
        ("dask_setup Basic", test_dask_setup_basic),
        ("Utils Module", test_utils_module),
        ("Sample Recipe", test_sample_recipe)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        print(f"\n🔍 Running {test_name}...")
        try:
            if test_name == "Import Tests":
                # Special handling for import test which returns tuple
                success, failures = test_func()
                results[test_name] = success
                if failures:
                    print(f"   Failures: {', '.join(failures)}")
            else:
                results[test_name] = test_func()
        except Exception as e:
            print(f"   ❌ Test {test_name} crashed: {e}")
            results[test_name] = False
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 TEST SUMMARY")
    print("=" * 60)
    
    passed = 0
    total = len(results)
    
    for test_name, success in results.items():
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"   {test_name:<20} {status}")
        if success:
            passed += 1
    
    print(f"\nResults: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! The recipe environment is ready.")
        return 0
    else:
        print(f"⚠️  {total - passed} tests failed. Check dependencies and setup.")
        return 1


def main():
    """Main entry point."""
    if len(sys.argv) > 1 and sys.argv[1] in ['-h', '--help']:
        print(__doc__)
        print("\nUsage:")
        print("  python test_recipes.py          # Run all tests")
        print("  python test_recipes.py --help   # Show this help")
        return 0
    
    return run_all_tests()


if __name__ == "__main__":
    sys.exit(main())