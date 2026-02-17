#!/usr/bin/env python
"""Recipe launcher script for dask_setup examples.

This script provides a convenient interface for running any of the dask_setup
recipe examples with proper environment setup and error handling.

Usage:
    python run_recipe.py --list                    # List all available recipes
    python run_recipe.py 01_basic                  # Run basic usage example
    python run_recipe.py 08_real_world_science --help  # Get help for specific recipe
    python run_recipe.py --check-deps              # Check all dependencies
"""

import argparse
import importlib.util
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional


def get_available_recipes() -> Dict[str, Dict[str, str]]:
    """Get list of available recipes with metadata."""
    recipes_dir = Path(__file__).parent
    recipes = {}

    # Find all recipe directories
    for item in recipes_dir.iterdir():
        if item.is_dir() and item.name.startswith(
            ("01_", "02_", "03_", "04_", "05_", "06_", "07_", "08_", "09_", "10_")
        ):
            # Find the main Python script
            python_files = list(item.glob("*.py"))
            main_script = None

            for py_file in python_files:
                if py_file.name != "__init__.py":
                    main_script = py_file
                    break

            if main_script:
                # Extract description from docstring
                description = "No description available"
                try:
                    with open(main_script, "r") as f:
                        content = f.read()
                        # Look for the first triple-quoted string
                        if '"""' in content:
                            start = content.find('"""')
                            end = content.find('"""', start + 3)
                            if end != -1:
                                docstring = content[start + 3 : end].strip()
                                # Get first line as description
                                description = docstring.split("\n")[0].strip()
                except Exception:
                    pass

                recipes[item.name] = {
                    "script": str(main_script),
                    "description": description,
                    "directory": str(item),
                }

    return recipes


def check_dependencies() -> Dict[str, bool]:
    """Check if all required dependencies are available."""
    print("🔍 Checking dependencies...")

    # Core dependencies (always required)
    core_deps = ["dask", "distributed", "xarray", "numpy", "pandas"]

    # Optional dependencies
    optional_deps = {
        "matplotlib": "Required for plotting in several recipes",
        "cartopy": "Required for advanced mapping in climate analysis",
        "scipy": "Required for statistical analysis and regridding",
        "zarr": "Required for Zarr format support",
        "rechunker": "Required for rechunking workflows",
        "psutil": "Required for system resource monitoring",
        "netcdf4": "Required for NetCDF file support",
        "h5netcdf": "Alternative NetCDF engine for better parallel performance",
    }

    results = {}

    print("\n📦 Core Dependencies:")
    for dep in core_deps:
        try:
            importlib.import_module(dep)
            results[dep] = True
            print(f"   ✅ {dep}")
        except ImportError:
            results[dep] = False
            print(f"   ❌ {dep} - REQUIRED")

    print("\n📦 Optional Dependencies:")
    for dep, description in optional_deps.items():
        try:
            importlib.import_module(dep)
            results[dep] = True
            print(f"   ✅ {dep}")
        except ImportError:
            results[dep] = False
            print(f"   ⚠️  {dep} - {description}")

    # Check for dask_setup specifically
    print("\n📦 dask_setup:")
    try:
        # Try to import from the local src directory
        src_path = Path(__file__).parents[2] / "src"
        if src_path.exists():
            sys.path.insert(0, str(src_path))
            import dask_setup

            results["dask_setup"] = True
            print(f"   ✅ dask_setup (local development version)")
        else:
            import dask_setup

            results["dask_setup"] = True
            print(f"   ✅ dask_setup (installed version)")
    except ImportError:
        results["dask_setup"] = False
        print(f"   ❌ dask_setup - REQUIRED (install with: pip install -e .)")

    return results


def run_recipe(recipe_name: str, recipe_args: List[str] = None) -> int:
    """Run a specific recipe with given arguments."""
    recipes = get_available_recipes()

    if recipe_name not in recipes:
        print(f"❌ Recipe '{recipe_name}' not found.")
        print(f"Available recipes: {', '.join(sorted(recipes.keys()))}")
        return 1

    recipe_info = recipes[recipe_name]
    script_path = recipe_info["script"]

    print(f"🚀 Running recipe: {recipe_name}")
    print(f"📄 Script: {Path(script_path).name}")
    print(f"📝 Description: {recipe_info['description']}")
    print("=" * 60)

    # Prepare command
    cmd = [sys.executable, script_path]
    if recipe_args:
        cmd.extend(recipe_args)

    # Change to recipe directory for relative imports
    recipe_dir = Path(recipe_info["directory"])
    original_cwd = os.getcwd()

    try:
        os.chdir(recipe_dir)

        # Add parent directories to path for imports
        parent_dir = recipe_dir.parent
        src_dir = parent_dir.parent.parent / "src"

        env = os.environ.copy()
        pythonpath = str(parent_dir) + os.pathsep + str(src_dir)
        if "PYTHONPATH" in env:
            pythonpath += os.pathsep + env["PYTHONPATH"]
        env["PYTHONPATH"] = pythonpath

        # Run the recipe
        result = subprocess.run(cmd, env=env)
        return result.returncode

    except KeyboardInterrupt:
        print("\n🛑 Recipe execution interrupted by user")
        return 130
    except Exception as e:
        print(f"❌ Failed to run recipe: {e}")
        return 1
    finally:
        os.chdir(original_cwd)


def list_recipes():
    """List all available recipes."""
    recipes = get_available_recipes()

    print("📚 AVAILABLE DASK_SETUP RECIPES")
    print("=" * 60)

    if not recipes:
        print("No recipes found!")
        return

    # Group recipes by category
    categories = {
        "01_": "🎯 Fundamentals",
        "02_": "⚙️  Configuration",
        "03_": "💾 Memory & Performance",
        "04_": "📁 Storage & I/O",
        "05_": "🔗 Integration",
        "06_": "🖥️  HPC Systems",
        "07_": "🐛 Troubleshooting",
        "08_": "🌍 Real-World Science",
        "09_": "⚡ Optimization",
        "10_": "🔧 Tool Integration",
    }

    current_category = None
    for recipe_name in sorted(recipes.keys()):
        # Determine category
        prefix = recipe_name[:3] + "_"
        category = categories.get(prefix, "📦 Other")

        if category != current_category:
            print(f"\n{category}")
            print("-" * 40)
            current_category = category

        recipe_info = recipes[recipe_name]
        print(f"  {recipe_name:<25} {recipe_info['description']}")

    print(f"\n💡 Usage Examples:")
    print(f"   python run_recipe.py {sorted(recipes.keys())[0]}           # Run first recipe")
    print(f"   python run_recipe.py {sorted(recipes.keys())[0]} --help    # Get recipe help")
    print(f"   python run_recipe.py --check-deps              # Check dependencies")


def main():
    """Main entry point for recipe launcher."""
    parser = argparse.ArgumentParser(
        description="Launch dask_setup recipe examples",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "recipe", nargs="?", help="Recipe to run (use --list to see available recipes)"
    )

    parser.add_argument("--list", action="store_true", help="List all available recipes")

    parser.add_argument("--check-deps", action="store_true", help="Check all dependencies")

    # Parse known args to allow passing args to recipes
    args, recipe_args = parser.parse_known_args()

    if args.list:
        list_recipes()
        return 0

    if args.check_deps:
        deps = check_dependencies()

        # Summary
        total_deps = len(deps)
        available_deps = sum(1 for v in deps.values() if v)

        print(f"\n📊 Summary: {available_deps}/{total_deps} dependencies available")

        # Check critical dependencies
        critical_missing = []
        for dep in ["dask_setup", "dask", "distributed", "xarray", "numpy"]:
            if not deps.get(dep, False):
                critical_missing.append(dep)

        if critical_missing:
            print(f"❌ Critical dependencies missing: {', '.join(critical_missing)}")
            print(f"💡 Install with: pip install {' '.join(critical_missing)}")
            return 1
        else:
            print("✅ All critical dependencies are available!")
            return 0

    if not args.recipe:
        print("❌ No recipe specified. Use --list to see available recipes.")
        parser.print_help()
        return 1

    return run_recipe(args.recipe, recipe_args)


if __name__ == "__main__":
    sys.exit(main())
