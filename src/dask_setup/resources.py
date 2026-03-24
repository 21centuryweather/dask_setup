"""Resource detection from PBS/SLURM/psutil."""

from __future__ import annotations

import os
import re

import psutil

from .exceptions import ResourceDetectionError
from .logging import get_logger
from .types import ResourceSpec

logger = get_logger("resources")


def validate_memory_value(value_bytes: int, context: str = "memory") -> None:
    """Validate memory value and emit warnings for problematic sizes.

    Args:
        value_bytes: Memory value in bytes to validate
        context: Description of where this memory value is used
    """
    if value_bytes < 32 * 1024 * 1024:  # 32 MiB
        logger.warning(
            f"Very small {context} value: {value_bytes / (1024**2):.1f} MiB. "
            f"This may be insufficient for typical Dask workloads."
        )
    elif value_bytes > 4 * 1024**4:  # 4 TiB
        logger.warning(
            f"Very large {context} value: {value_bytes / (1024**4):.1f} TiB. "
            f"Verify this is correct for your system."
        )


def _parse_mem_bytes_with_feedback(mem_str: str | None, context: str = "memory") -> int | None:
    """Parse memory string with detailed error feedback.

    Args:
        mem_str: Memory string to parse
        context: Context description for better error messages

    Returns:
        Memory in bytes, or None if parsing fails

    Raises:
        ResourceDetectionError: If parsing fails with helpful suggestions
    """
    result = _parse_mem_bytes(mem_str)
    if result is None and mem_str:
        raise ResourceDetectionError(
            f"Could not parse {context} value '{mem_str}'. "
            f"Use formats like '64GB', '1.5gib', '32768MB', or '98304' (treated as MB)."
        )
    return result


def _parse_mem_bytes(mem_str: str | None) -> int | None:
    """Parse memory specification string to bytes.

    Supports formats like:
    - Pure numbers: "98304" (treated as MB for SLURM compatibility)
    - With units: "300gb", "64GB", "1.5gib", "32768mb"
    - With spaces: "16 GB", "1.5 TiB", "512 MB"
    - All whitespace combinations and case variations

    Binary vs Decimal units:
    - k/kb = 1000, ki/kib = 1024
    - m/mb = 1000², mi/mib = 1024²
    - g/gb = 1000³, gi/gib = 1024³
    - t/tb = 1000⁴, ti/tib = 1024⁴
    - p/pb = 1000⁵, pi/pib = 1024⁵
    - e/eb = 1000⁶, ei/eib = 1024⁶

    Args:
        mem_str: Memory string to parse

    Returns:
        Memory in bytes, or None if parsing fails or value exceeds 8 EiB
    """
    if not mem_str:
        return None

    # Maximum supported memory: 8 EiB (exbibytes) to prevent overflow issues
    MAX_MEMORY_BYTES = 8 * (1024**6)  # 8 EiB

    try:
        # First check for invalid patterns before normalization
        original_str = mem_str

        # Check for multiple numbers separated by spaces/text (e.g., "1 2gb", "5 10mb")
        if re.search(r"\d+\s+\d+", mem_str):
            logger.debug(f"Invalid memory format (multiple numbers): '{original_str}'")
            return None

        # Normalize: strip all whitespace and convert to lowercase
        mem_str = re.sub(r"\s+", "", mem_str.strip().lower())

        if not mem_str:
            logger.debug(f"Empty memory string after normalization: '{original_str}'")
            return None

        # Handle pure numbers (assume MB for SLURM compatibility)
        if mem_str.isdigit():
            result = int(mem_str) * 1024 * 1024  # MB to bytes
            if result > MAX_MEMORY_BYTES:
                logger.warning(f"Memory value {original_str} MB exceeds maximum supported (8 EiB)")
                return None
            return result

        # Use regex to parse size with units (handles space separation after normalization)
        # Only allow digits, optional decimal point, followed by unit - no extra characters
        match = re.match(r"^(\d+(?:\.\d+)?)([kmgtpe]?i?b?)$", mem_str)
        if not match:
            logger.debug(
                f"Could not parse memory format: '{original_str}'. Use formats like '64GB', '1.5gib', or '98304' (MB)"
            )
            return None

        size_str, unit = match.groups()
        size = float(size_str)

        if size < 0:
            logger.debug(f"Negative memory value not supported: '{original_str}'")
            return None

        # Handle different unit formats
        unit = unit.lower()
        if unit in ("", "b"):
            multiplier = 1
        elif unit in ("k", "kb"):
            multiplier = 1000
        elif unit in ("ki", "kib"):
            multiplier = 1024
        elif unit in ("m", "mb"):
            multiplier = 1000**2
        elif unit in ("mi", "mib"):
            multiplier = 1024**2
        elif unit in ("g", "gb"):
            multiplier = 1000**3
        elif unit in ("gi", "gib"):
            multiplier = 1024**3
        elif unit in ("t", "tb"):
            multiplier = 1000**4
        elif unit in ("ti", "tib"):
            multiplier = 1024**4
        elif unit in ("p", "pb"):
            multiplier = 1000**5
        elif unit in ("pi", "pib"):
            multiplier = 1024**5
        elif unit in ("e", "eb"):
            multiplier = 1000**6
        elif unit in ("ei", "eib"):
            multiplier = 1024**6
        else:
            logger.debug(f"Unknown memory unit in '{original_str}': '{unit}'")
            return None

        # Calculate result and check for overflow
        try:
            result = int(size * multiplier)
            if result > MAX_MEMORY_BYTES:
                logger.warning(f"Memory value '{original_str}' exceeds maximum supported (8 EiB)")
                return None
            return result
        except OverflowError:
            logger.warning(f"Memory value '{original_str}' causes overflow")
            return None

    except (ValueError, AttributeError) as e:
        logger.debug(f"Failed to parse memory string '{original_str}': {e}")
        return None


def _parse_pbs_mem_bytes(mem_str: str | None) -> int | None:
    """Parse a PBS memory string to bytes.

    Bare integers are interpreted using a magnitude heuristic:

    * If the value is **>= 1,000,000,000** it is already in bytes (e.g. NCI
      Gadi sets ``PBS_MEM`` / ``PBS_VMEM`` to the raw byte count such as
      ``"532575944704"`` for 496 GiB).
    * If the value is **< 1,000,000,000** it is treated as **mebibytes** (MiB),
      which is the convention used by many other PBS Pro sites (e.g.
      ``"16384"`` for 16 GiB).

    Strings that include a unit suffix (e.g. ``"512gb"``, ``"496gib"``) are
    forwarded to :func:`_parse_mem_bytes` unchanged.

    Args:
        mem_str: Memory string from a PBS environment variable, or ``None``.

    Returns:
        Memory in bytes, or ``None`` if *mem_str* is empty / unparseable.
    """
    if not mem_str:
        return None
    stripped = mem_str.strip()
    if stripped.isdigit():
        value = int(stripped)
        if value >= 1_000_000_000:
            # Large value — already expressed in bytes (e.g. Gadi PBS_MEM)
            return value
        else:
            # Small/moderate value — treat as MiB (common PBS Pro convention)
            return value * 1024 * 1024
    # Has a unit suffix — use the general parser.
    return _parse_mem_bytes(stripped)


def _detect_slurm_resources() -> ResourceSpec | None:
    """Detect resources from SLURM environment variables.

    Returns:
        ResourceSpec if SLURM environment is detected, None otherwise
    """
    slurm_cpus = os.getenv("SLURM_CPUS_ON_NODE")
    slurm_mem_total = os.getenv("SLURM_MEM_PER_NODE")
    slurm_mem_per_cpu = os.getenv("SLURM_MEM_PER_CPU")

    if not slurm_cpus:
        return None

    try:
        total_cores = int(slurm_cpus)
    except ValueError:
        logger.debug(f"Invalid SLURM_CPUS_ON_NODE value: '{slurm_cpus}'")
        return None

    # Try total memory first, then per-CPU memory using improved parser
    total_mem_bytes = None

    if slurm_mem_total:
        # Try parsing with improved parser first
        total_mem_bytes = _parse_mem_bytes(slurm_mem_total)
        if total_mem_bytes is None and slurm_mem_total.isdigit():
            # Fallback to old logic for pure digits (SLURM MB format)
            total_mem_bytes = int(slurm_mem_total) * 1024 * 1024  # MB to bytes

    if total_mem_bytes is None and slurm_mem_per_cpu:
        # Try parsing per-CPU memory with improved parser
        per_cpu_bytes = _parse_mem_bytes(slurm_mem_per_cpu)
        if per_cpu_bytes is not None:
            total_mem_bytes = per_cpu_bytes * total_cores
        elif slurm_mem_per_cpu.isdigit():
            # Fallback to old logic for pure digits (SLURM MB format)
            total_mem_bytes = int(slurm_mem_per_cpu) * total_cores * 1024 * 1024  # MB to bytes

    if total_mem_bytes is None:
        # Fall back to psutil for memory if SLURM memory info is unavailable
        logger.debug("SLURM memory detection failed, falling back to psutil")
        total_mem_bytes = psutil.virtual_memory().total

    return ResourceSpec(
        total_cores=total_cores, total_mem_bytes=total_mem_bytes, detection_method="SLURM"
    )


def _detect_pbs_resources() -> ResourceSpec | None:
    """Detect resources from PBS environment variables.

    Returns:
        ResourceSpec if PBS environment is detected, None otherwise
    """
    pbs_ncpus = os.getenv("NCPUS") or os.getenv("PBS_NCPUS")
    pbs_mem = os.getenv("PBS_VMEM") or os.getenv("PBS_MEM")

    if not pbs_ncpus or not pbs_ncpus.isdigit():
        return None

    try:
        total_cores = int(pbs_ncpus)
    except ValueError:
        return None

    total_mem_bytes = _parse_pbs_mem_bytes(pbs_mem)
    if total_mem_bytes is None:
        # Fall back to psutil for memory if PBS memory info is unavailable
        total_mem_bytes = psutil.virtual_memory().total

    return ResourceSpec(
        total_cores=total_cores, total_mem_bytes=total_mem_bytes, detection_method="PBS"
    )


def _detect_psutil_resources() -> ResourceSpec:
    """Detect resources using psutil as fallback.

    Returns:
        ResourceSpec from psutil detection

    Raises:
        ResourceDetectionError: If psutil detection fails
    """
    try:
        total_cores = psutil.cpu_count(logical=True)
        total_mem_bytes = psutil.virtual_memory().total

        if total_cores is None or total_cores <= 0:
            raise ResourceDetectionError("Could not detect CPU cores from psutil")

        if total_mem_bytes <= 0:
            raise ResourceDetectionError("Could not detect memory from psutil")

        return ResourceSpec(
            total_cores=total_cores, total_mem_bytes=total_mem_bytes, detection_method="psutil"
        )

    except Exception as e:
        raise ResourceDetectionError(f"psutil resource detection failed: {e}") from e


_FALLBACK_CORES = 2
_FALLBACK_MEM_BYTES = 8 * 1024**3  # 8 GiB — safe minimum for a single Dask worker


def _detect_fallback_resources() -> ResourceSpec:
    """Return ultra-conservative hardcoded resources when all detection fails.

    These values are intentionally cautious (2 cores, 8 GiB) so that the
    resulting cluster is safe to run on almost any machine, even if the
    resulting performance is suboptimal.

    Returns:
        ResourceSpec with detection_method="fallback"
    """
    return ResourceSpec(
        total_cores=_FALLBACK_CORES,
        total_mem_bytes=_FALLBACK_MEM_BYTES,
        detection_method="fallback",
    )


def detect_resources(fallback: bool = False) -> ResourceSpec:
    """Detect available CPU cores and memory from environment.

    Detection priority:
    1. SLURM environment variables (SLURM_CPUS_ON_NODE, SLURM_MEM_PER_NODE)
    2. PBS environment variables (NCPUS/PBS_NCPUS, PBS_MEM/PBS_VMEM)
    3. psutil fallback
    4. Hardcoded conservative defaults — only when ``fallback=True``

    Args:
        fallback: If True, return conservative hardcoded defaults (2 cores,
            8 GiB) instead of raising :exc:`ResourceDetectionError` when all
            detection methods fail.  A warning is logged in this case.
            Default: False (raise on failure, existing behaviour).

    Returns:
        ResourceSpec with detected resources

    Raises:
        ResourceDetectionError: If all detection methods fail and
            ``fallback=False`` (default).
    """
    # Try SLURM first
    slurm_resources = _detect_slurm_resources()
    if slurm_resources is not None:
        logger.info(
            "Resources detected via SLURM",
            total_cores=slurm_resources.total_cores,
            total_mem_gib=f"{slurm_resources.total_mem_bytes / (1024**3):.1f}",
        )
        return slurm_resources

    # Try PBS next
    pbs_resources = _detect_pbs_resources()
    if pbs_resources is not None:
        logger.info(
            "Resources detected via PBS",
            total_cores=pbs_resources.total_cores,
            total_mem_gib=f"{pbs_resources.total_mem_bytes / (1024**3):.1f}",
        )
        return pbs_resources

    # Fall back to psutil
    try:
        psutil_resources = _detect_psutil_resources()
        logger.info(
            "Resources detected via psutil (no HPC scheduler detected)",
            total_cores=psutil_resources.total_cores,
            total_mem_gib=f"{psutil_resources.total_mem_bytes / (1024**3):.1f}",
        )
        return psutil_resources
    except ResourceDetectionError:
        if fallback:
            fb = _detect_fallback_resources()
            logger.warning(
                "All resource detection methods failed — using conservative hardcoded fallback. "
                "Set fallback_on_detection_failure=False (or fix the environment) to see the "
                "original error.",
                fallback_cores=fb.total_cores,
                fallback_mem_gib=f"{fb.total_mem_bytes / (1024**3):.1f}",
            )
            return fb
        raise
