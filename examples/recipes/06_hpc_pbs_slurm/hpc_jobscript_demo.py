#!/usr/bin/env python
"""HPC job script generation and PBS/SLURM integration demonstration.

This recipe shows how to integrate dask_setup with HPC schedulers (PBS/SLURM),
including automatic job script generation, resource detection, SSH tunnel setup,
and best practices for running Dask on HPC systems.

Requirements:
- dask_setup, dask, distributed
- psutil (for resource detection)
- Running on HPC system (optional, will simulate otherwise)

Outputs:
- Generated PBS/SLURM job scripts
- SSH tunnel commands for dashboard access
- Resource detection and recommendations
- HPC environment analysis

Key Learning Points:
- Automatic PBS/SLURM job script generation
- Resource detection from scheduler environment
- Dashboard access via SSH tunneling
- HPC-specific configuration optimization
- Job monitoring and status checking
"""

import argparse
import logging
import os
import socket
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Add the parent directory to path so we can import dask_setup and utils
sys.path.insert(0, str(Path(__file__).parents[3] / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    import psutil

    from dask_setup import setup_dask_client
    from utils import format_duration, print_dashboard_instructions, timer
except ImportError as e:
    print(f"❌ Missing required dependency: {e}")
    print("Please install: pip install dask-setup dask distributed psutil")
    sys.exit(1)


def setup_logging(verbose: bool = False) -> None:
    """Configure logging for the recipe."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%H:%M:%S",
    )


def detect_hpc_environment() -> Dict[str, any]:
    """Detect HPC scheduler environment and available resources.

    Returns:
        Dictionary with HPC environment information
    """
    env_info = {
        "scheduler": None,
        "job_id": None,
        "hostname": socket.gethostname(),
        "detected_resources": {},
        "environment_variables": {},
        "recommendations": [],
    }

    # Check for PBS environment
    pbs_vars = {
        "PBS_JOBID": os.environ.get("PBS_JOBID"),
        "PBS_JOBFS": os.environ.get("PBS_JOBFS"),
        "PBS_NCPUS": os.environ.get("PBS_NCPUS"),
        "NCPUS": os.environ.get("NCPUS"),
        "PBS_MEM": os.environ.get("PBS_MEM"),
        "PBS_VMEM": os.environ.get("PBS_VMEM"),
        "PBS_QUEUE": os.environ.get("PBS_QUEUE"),
        "PBS_NODEFILE": os.environ.get("PBS_NODEFILE"),
    }

    # Check for SLURM environment
    slurm_vars = {
        "SLURM_JOB_ID": os.environ.get("SLURM_JOB_ID"),
        "SLURM_CPUS_ON_NODE": os.environ.get("SLURM_CPUS_ON_NODE"),
        "SLURM_MEM_PER_NODE": os.environ.get("SLURM_MEM_PER_NODE"),
        "SLURM_MEM_PER_CPU": os.environ.get("SLURM_MEM_PER_CPU"),
        "SLURM_PARTITION": os.environ.get("SLURM_PARTITION"),
        "SLURM_NODELIST": os.environ.get("SLURM_NODELIST"),
    }

    # Determine scheduler type
    if any(pbs_vars.values()):
        env_info["scheduler"] = "PBS"
        env_info["job_id"] = pbs_vars["PBS_JOBID"]
        env_info["environment_variables"] = {k: v for k, v in pbs_vars.items() if v}

        # Parse PBS resources
        if pbs_vars["NCPUS"] or pbs_vars["PBS_NCPUS"]:
            ncpus = int(pbs_vars["NCPUS"] or pbs_vars["PBS_NCPUS"])
            env_info["detected_resources"]["cpus"] = ncpus

        if pbs_vars["PBS_MEM"] or pbs_vars["PBS_VMEM"]:
            mem_str = pbs_vars["PBS_MEM"] or pbs_vars["PBS_VMEM"]
            # Parse memory (e.g., "300gb", "64000mb")
            if "gb" in mem_str.lower():
                memory_gb = float(mem_str.lower().replace("gb", ""))
            elif "mb" in mem_str.lower():
                memory_gb = float(mem_str.lower().replace("mb", "")) / 1024
            else:
                memory_gb = None
            if memory_gb:
                env_info["detected_resources"]["memory_gb"] = memory_gb

    elif any(slurm_vars.values()):
        env_info["scheduler"] = "SLURM"
        env_info["job_id"] = slurm_vars["SLURM_JOB_ID"]
        env_info["environment_variables"] = {k: v for k, v in slurm_vars.items() if v}

        # Parse SLURM resources
        if slurm_vars["SLURM_CPUS_ON_NODE"]:
            env_info["detected_resources"]["cpus"] = int(slurm_vars["SLURM_CPUS_ON_NODE"])

        if slurm_vars["SLURM_MEM_PER_NODE"]:
            # SLURM memory is in MB
            memory_mb = int(slurm_vars["SLURM_MEM_PER_NODE"])
            env_info["detected_resources"]["memory_gb"] = memory_mb / 1024
        elif slurm_vars["SLURM_MEM_PER_CPU"] and env_info["detected_resources"].get("cpus"):
            memory_per_cpu_mb = int(slurm_vars["SLURM_MEM_PER_CPU"])
            total_memory_gb = (memory_per_cpu_mb * env_info["detected_resources"]["cpus"]) / 1024
            env_info["detected_resources"]["memory_gb"] = total_memory_gb

    else:
        env_info["scheduler"] = "Local/Unknown"
        env_info["recommendations"].append("Not running under PBS or SLURM scheduler")

    # Get system resources as fallback
    try:
        system_cpus = psutil.cpu_count()
        system_memory_gb = psutil.virtual_memory().total / (1024**3)

        if "cpus" not in env_info["detected_resources"]:
            env_info["detected_resources"]["cpus"] = system_cpus
            env_info["recommendations"].append(
                f"Using system CPU count ({system_cpus}) - consider requesting specific resources"
            )

        if "memory_gb" not in env_info["detected_resources"]:
            env_info["detected_resources"]["memory_gb"] = system_memory_gb
            env_info["recommendations"].append(
                f"Using system memory ({system_memory_gb:.1f} GB) - consider requesting specific resources"
            )
    except Exception:
        pass

    return env_info


def generate_pbs_script(
    cpus: int = 48,
    memory_gb: int = 190,
    walltime: str = "4:00:00",
    queue: str = "normalsr",
    jobfs_gb: int = 100,
    storage_paths: List[str] = None,
    script_name: str = "dask_job",
    python_script: str = "my_analysis.py",
) -> str:
    """Generate a PBS job script for dask_setup.

    Args:
        cpus: Number of CPUs to request
        memory_gb: Memory in GB to request
        walltime: Wall time limit
        queue: PBS queue name
        jobfs_gb: Job filesystem storage in GB
        storage_paths: List of storage paths to access
        script_name: Name of the job script
        python_script: Python script to execute

    Returns:
        PBS script content as string
    """
    if storage_paths is None:
        storage_paths = ["gdata/hh5"]

    storage_str = "+".join(storage_paths)

    script = f"""#!/bin/bash
#PBS -P {os.environ.get("PROJECT", "your_project")}
#PBS -q {queue}
#PBS -l ncpus={cpus}
#PBS -l mem={memory_gb}gb
#PBS -l jobfs={jobfs_gb}gb
#PBS -l walltime={walltime}
#PBS -l storage={storage_str}
#PBS -l wd
#PBS -N {script_name}
#PBS -o {script_name}.out
#PBS -e {script_name}.err

# Load required modules
module use /g/data/hh5/public/modules/
module load conda_concept/analysis3-unstable

# Set up environment for dask_setup
export TMPDIR="$PBS_JOBFS"
export OMP_NUM_THREADS=1

# Print job information
echo "Job ID: $PBS_JOBID"
echo "Queue: $PBS_QUEUE"  
echo "Nodes: $(cat $PBS_NODEFILE | sort | uniq | wc -l)"
echo "CPUs: $PBS_NCPUS"
echo "Memory: {memory_gb}GB"
echo "JobFS: $PBS_JOBFS ({jobfs_gb}GB)"
echo "Working directory: $(pwd)"
echo "Started at: $(date)"

# Print resource utilization function
print_resource_usage() {{
    echo "=== Resource Usage ==="
    echo "CPU Usage: $(top -bn1 | grep "Cpu(s)" | sed "s/.*, *\\([0-9.]*\\)%* id.*/\\1/" | awk '{{print 100 - $1"%"}}')"
    echo "Memory: $(free -h | awk '/^Mem:/ {{print $3 "/" $2 " (" $3/$2*100 "%)"}}')"
    echo "JobFS usage: $(df -h $PBS_JOBFS | tail -1 | awk '{{print $3 "/" $2 " (" $5 ")"}}')"
    echo "===================="
}}

# Set up signal handlers for monitoring
trap print_resource_usage EXIT
trap 'echo "Job interrupted at $(date)"; print_resource_usage; exit 130' INT TERM

# Run the Python script
echo "Starting Python script: {python_script}"
python {python_script}

# Print final resource usage
print_resource_usage
echo "Completed at: $(date)"

# Clean up temporary files (dask_setup handles this automatically)
# The temp directory $PBS_JOBFS is automatically cleaned by PBS
"""

    return script


def generate_slurm_script(
    cpus: int = 48,
    memory_gb: int = 190,
    walltime: str = "4:00:00",
    partition: str = "normal",
    script_name: str = "dask_job",
    python_script: str = "my_analysis.py",
) -> str:
    """Generate a SLURM job script for dask_setup.

    Args:
        cpus: Number of CPUs to request
        memory_gb: Memory in GB to request
        walltime: Wall time limit
        partition: SLURM partition name
        script_name: Name of the job script
        python_script: Python script to execute

    Returns:
        SLURM script content as string
    """
    script = f"""#!/bin/bash
#SBATCH --job-name={script_name}
#SBATCH --partition={partition}
#SBATCH --ntasks=1
#SBATCH --cpus-per-task={cpus}
#SBATCH --mem={memory_gb}G
#SBATCH --time={walltime}
#SBATCH --output={script_name}_%j.out
#SBATCH --error={script_name}_%j.err

# Load required modules (adjust for your system)
module load python/3.11
module load dask

# Set up environment for dask_setup
export TMPDIR="/tmp/slurm_$SLURM_JOB_ID"
mkdir -p $TMPDIR
export OMP_NUM_THREADS=1

# Print job information
echo "Job ID: $SLURM_JOB_ID"
echo "Partition: $SLURM_JOB_PARTITION"
echo "Node: $SLURMD_NODENAME"
echo "CPUs: $SLURM_CPUS_ON_NODE"
echo "Memory: {memory_gb}GB"
echo "Temp dir: $TMPDIR"
echo "Working directory: $(pwd)"
echo "Started at: $(date)"

# Print resource utilization function
print_resource_usage() {{
    echo "=== Resource Usage ==="
    if command -v sstat > /dev/null 2>&1; then
        sstat -j $SLURM_JOB_ID --format=AveCPU,AveRSS,MaxRSS 2>/dev/null || echo "sstat not available"
    fi
    echo "Memory: $(free -h | awk '/^Mem:/ {{print $3 "/" $2 " (" $3/$2*100 "%)"}}')"
    echo "Temp usage: $(du -sh $TMPDIR 2>/dev/null || echo 'N/A')"
    echo "===================="
}}

# Set up signal handlers
trap print_resource_usage EXIT
trap 'echo "Job interrupted at $(date)"; print_resource_usage; exit 130' INT TERM

# Run the Python script
echo "Starting Python script: {python_script}"
python {python_script}

# Print final resource usage
print_resource_usage
echo "Completed at: $(date)"

# Clean up temporary directory
rm -rf $TMPDIR
"""

    return script


def demonstrate_ssh_tunneling(client, cluster) -> None:
    """Demonstrate SSH tunneling setup for dashboard access.

    Args:
        client: Dask distributed client
        cluster: Dask LocalCluster instance
    """
    print("\n" + "=" * 60)
    print("🔗 SSH TUNNEL SETUP FOR DASHBOARD ACCESS")
    print("=" * 60)

    if not hasattr(cluster, "dashboard_link") or not cluster.dashboard_link:
        print("❌ Dashboard not available")
        return

    # Parse dashboard URL
    dashboard_url = cluster.dashboard_link
    try:
        from urllib.parse import urlparse

        parsed = urlparse(dashboard_url)
        dashboard_port = parsed.port or 8787
        hostname = socket.gethostname()
    except Exception:
        dashboard_port = 8787
        hostname = socket.gethostname()

    print(f"🖥️  Dashboard URL: {dashboard_url}")
    print(f"📡 Running on: {hostname}:{dashboard_port}")

    # Check if we're on a known HPC system
    hpc_systems = {
        "gadi": "gadi.nci.org.au",
        "raijin": "raijin.nci.org.au",
        "magnus": "magnus.pawsey.org.au",
        "topaz": "topaz.pawsey.org.au",
    }

    login_node = None
    for hpc_name, login_hostname in hpc_systems.items():
        if hpc_name in hostname.lower():
            login_node = login_hostname
            break

    if login_node:
        print(f"\n🔗 SSH Tunnel Commands:")
        print(f"   From your local machine, run:")
        print(f"   ssh -N -L 8787:{hostname}:{dashboard_port} {login_node}")
        print(f"   ")
        print(f"   Then open in your browser:")
        print(f"   http://localhost:8787")

        print(f"\n📱 Alternative (if compute node access is restricted):")
        print(f"   ssh -N -L 8787:localhost:{dashboard_port} {login_node}")
        print(f"   (This requires the dashboard to be accessible from the login node)")
    else:
        print(f"\n🔗 Generic SSH Tunnel Command:")
        print(f"   ssh -N -L 8787:{hostname}:{dashboard_port} <your_hpc_login_node>")
        print(f"   ")
        print(f"   Then open: http://localhost:8787")

    print(f"\n💡 Dashboard Features:")
    print(f"   • Status: Real-time worker and task information")
    print(f"   • Task Stream: Live view of task execution")
    print(f"   • Memory: Worker memory usage and spill activity")
    print(f"   • Progress: Long-running operation progress bars")
    print(f"   • Profile: Performance profiling information")

    print(f"\n⚠️  SSH Tunnel Troubleshooting:")
    print(f"   • Check if port {dashboard_port} is open on {hostname}")
    print(f"   • Verify SSH key authentication works")
    print(f"   • Try different local port if 8787 is busy: -L 8788:...")
    print(f"   • Check HPC firewall rules for compute node access")


def demonstrate_job_monitoring() -> None:
    """Demonstrate job monitoring commands for PBS and SLURM."""
    print("\n" + "=" * 60)
    print("📊 JOB MONITORING COMMANDS")
    print("=" * 60)

    # Detect current environment
    pbs_job = os.environ.get("PBS_JOBID")
    slurm_job = os.environ.get("SLURM_JOB_ID")

    if pbs_job:
        print(f"🐚 PBS Job Monitoring (Job ID: {pbs_job}):")
        print(f"   Current job status:")
        print(f"     qstat -f {pbs_job}")
        print(f"   ")
        print(f"   Resource usage:")
        print(f"     qstat -f {pbs_job} | grep resources_used")
        print(f"   ")
        print(f"   Queue information:")
        print(f"     qstat -Q")
        print(f"   ")
        print(f"   Node information:")
        print(f"     cat $PBS_NODEFILE")
        print(f"   ")
        print(f"   Job filesystem usage:")
        print(f"     df -h $PBS_JOBFS")

        # Try to run some monitoring commands if we're actually in PBS
        print(f"\n📈 Current Job Information:")
        try:
            import subprocess

            result = subprocess.run(
                ["qstat", "-f", pbs_job], capture_output=True, text=True, timeout=10
            )
            if result.returncode == 0:
                # Parse interesting fields
                lines = result.stdout.split("\n")
                for line in lines:
                    if any(
                        keyword in line.lower()
                        for keyword in [
                            "job_name",
                            "queue",
                            "resources_used.walltime",
                            "resources_used.mem",
                            "job_state",
                        ]
                    ):
                        print(f"     {line.strip()}")
            else:
                print(f"     qstat command failed (may not be available)")
        except Exception:
            print(f"     qstat not available or job not found")

    elif slurm_job:
        print(f"🔰 SLURM Job Monitoring (Job ID: {slurm_job}):")
        print(f"   Current job status:")
        print(f"     squeue -j {slurm_job}")
        print(f"   ")
        print(f"   Detailed job information:")
        print(f"     scontrol show job {slurm_job}")
        print(f"   ")
        print(f"   Resource usage:")
        print(f"     sstat -j {slurm_job} --format=AveCPU,AveRSS,MaxRSS")
        print(f"   ")
        print(f"   Partition information:")
        print(f"     sinfo")
        print(f"   ")
        print(f"   Node information:")
        print(f"     squeue -j {slurm_job} -o %N")

    else:
        print(f"💻 Local System Monitoring:")
        print(f"   System resource usage:")
        print(f"     htop  # Interactive process viewer")
        print(f"     free -h  # Memory usage")
        print(f"     df -h  # Disk usage")
        print(f"   ")
        print(f"   Dask-specific monitoring:")
        print(f"     Dashboard: http://localhost:8787 (if enabled)")
        print(f"     ps aux | grep python  # Find Python processes")


def generate_example_analysis_script(output_file: Path) -> None:
    """Generate an example Python analysis script using dask_setup.

    Args:
        output_file: Path to write the example script
    """
    script_content = '''#!/usr/bin/env python
"""Example analysis script using dask_setup for HPC systems.

This script demonstrates a typical scientific workflow using dask_setup
on HPC systems with proper resource management and monitoring.
"""

import sys
import time
from pathlib import Path

import dask.array as da
import numpy as np
import xarray as xr

from dask_setup import setup_dask_client

def main():
    """Main analysis function."""
    print("🔬 Starting scientific analysis...")
    
    # Setup Dask cluster (dask_setup automatically detects PBS/SLURM resources)
    client, cluster, temp_dir = setup_dask_client(
        workload_type="cpu",  # CPU-intensive analysis
        reserve_mem_gb=60,    # Reserve memory for OS and I/O
        dashboard=True        # Enable dashboard for monitoring
    )
    
    print(f"📊 Cluster info: {len(client.scheduler_info()['workers'])} workers")
    print(f"📁 Temp directory: {temp_dir}")
    
    try:
        # Simulate loading a large dataset
        print("📂 Loading dataset...")
        
        # Create a large synthetic dataset (adjust size based on your needs)
        shape = (365*5, 180, 360)  # 5 years of daily global data
        chunks = (365, 90, 180)    # ~512MB chunks
        
        # Simulate climate data with multiple variables
        ds = xr.Dataset({
            'temperature': (['time', 'lat', 'lon'], 
                          da.random.random(shape, chunks=chunks, dtype='float32')),
            'precipitation': (['time', 'lat', 'lon'], 
                            da.random.random(shape, chunks=chunks, dtype='float32'))
        }, coords={
            'time': np.arange(shape[0]),
            'lat': np.linspace(-90, 90, shape[1]),
            'lon': np.linspace(-180, 180, shape[2])
        })
        
        print(f"   Dataset size: {ds.nbytes / (1024**3):.1f} GB")
        print(f"   Chunks: {ds.temperature.chunks}")
        
        # Perform some analysis
        print("🧮 Running analysis...")
        
        # Calculate climatology (multi-year means)
        start_time = time.perf_counter()
        
        temp_climatology = ds.temperature.groupby('time.month').mean()
        precip_climatology = ds.precipitation.groupby('time.month').mean()
        
        # Compute results
        temp_clim_result = temp_climatology.compute()
        precip_clim_result = precip_climatology.compute()
        
        analysis_time = time.perf_counter() - start_time
        
        print(f"✅ Analysis completed in {analysis_time:.1f} seconds")
        print(f"   Temperature climatology shape: {temp_clim_result.shape}")
        print(f"   Precipitation climatology shape: {precip_clim_result.shape}")
        
        # Save results (optional)
        output_dir = Path("results")
        output_dir.mkdir(exist_ok=True)
        
        print("💾 Saving results...")
        temp_clim_result.to_netcdf(output_dir / "temperature_climatology.nc")
        precip_clim_result.to_netcdf(output_dir / "precipitation_climatology.nc")
        
        print("🎉 Analysis completed successfully!")
        
    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        raise
    
    finally:
        # Clean shutdown
        print("🧹 Shutting down cluster...")
        client.close()
        cluster.close()

if __name__ == "__main__":
    main()
'''

    with open(output_file, "w") as f:
        f.write(script_content)

    # Make script executable
    output_file.chmod(0o755)
    print(f"📝 Example analysis script created: {output_file}")


def main():
    """Main function demonstrating HPC integration."""
    parser = argparse.ArgumentParser(
        description="Demonstrate HPC job script generation and integration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--generate-scripts", action="store_true", help="Generate example PBS and SLURM job scripts"
    )

    parser.add_argument(
        "--cpus",
        type=int,
        default=None,
        help="Number of CPUs to request (auto-detect if not specified)",
    )

    parser.add_argument(
        "--memory",
        type=int,
        default=None,
        help="Memory in GB to request (auto-detect if not specified)",
    )

    parser.add_argument("--walltime", default="4:00:00", help="Wall time limit (default: 4:00:00)")

    parser.add_argument(
        "--test-cluster", action="store_true", help="Test cluster creation and dashboard access"
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).parent,
        help="Output directory for generated scripts",
    )

    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")

    args = parser.parse_args()

    setup_logging(args.verbose)

    print("🖥️  HPC JOB SCRIPT GENERATION AND INTEGRATION")
    print("=" * 60)

    try:
        # Detect HPC environment
        hpc_env = detect_hpc_environment()

        print(f"\n🔍 HPC Environment Detection:")
        print(f"   Scheduler: {hpc_env['scheduler']}")
        print(f"   Hostname: {hpc_env['hostname']}")

        if hpc_env["job_id"]:
            print(f"   Job ID: {hpc_env['job_id']}")

        if hpc_env["detected_resources"]:
            resources = hpc_env["detected_resources"]
            print(f"   Detected CPUs: {resources.get('cpus', 'Unknown')}")
            print(f"   Detected Memory: {resources.get('memory_gb', 'Unknown')} GB")

        if hpc_env["recommendations"]:
            print(f"\n💡 Recommendations:")
            for rec in hpc_env["recommendations"]:
                print(f"   • {rec}")

        # Use detected or specified resources
        cpus = args.cpus or hpc_env["detected_resources"].get("cpus", 48)
        memory = args.memory or int(hpc_env["detected_resources"].get("memory_gb", 190))

        # Generate job scripts
        if args.generate_scripts:
            print(f"\n📝 Generating job scripts...")

            # PBS script
            pbs_script = generate_pbs_script(
                cpus=cpus,
                memory_gb=memory,
                walltime=args.walltime,
                script_name="dask_analysis",
                python_script="analysis.py",
            )

            pbs_file = args.output_dir / "dask_analysis.pbs"
            with open(pbs_file, "w") as f:
                f.write(pbs_script)
            print(f"   PBS script: {pbs_file}")

            # SLURM script
            slurm_script = generate_slurm_script(
                cpus=cpus,
                memory_gb=memory,
                walltime=args.walltime,
                script_name="dask_analysis",
                python_script="analysis.py",
            )

            slurm_file = args.output_dir / "dask_analysis.slurm"
            with open(slurm_file, "w") as f:
                f.write(slurm_script)
            print(f"   SLURM script: {slurm_file}")

            # Generate example analysis script
            analysis_file = args.output_dir / "analysis.py"
            generate_example_analysis_script(analysis_file)

            print(f"\n🚀 Usage Instructions:")
            print(f"   PBS: qsub {pbs_file.name}")
            print(f"   SLURM: sbatch {slurm_file.name}")

        # Test cluster creation
        if args.test_cluster:
            print(f"\n🧪 Testing cluster creation...")

            client, cluster, temp_dir = setup_dask_client(
                workload_type="mixed",
                max_workers=min(4, cpus),  # Limit for testing
                reserve_mem_gb=min(30, memory // 2),
                dashboard=True,
            )

            print(f"✅ Cluster created successfully:")
            print(f"   Workers: {len(client.scheduler_info()['workers'])}")
            print(f"   Temp directory: {temp_dir}")

            # Demonstrate SSH tunneling
            demonstrate_ssh_tunneling(client, cluster)

            # Quick performance test
            print(f"\n⚡ Running quick performance test...")
            with timer("Performance test"):
                import dask.array as da

                x = da.random.random((1000, 1000), chunks=(200, 200))
                result = (x + x.T).mean().compute()
                print(f"   Test result: {result:.6f}")

            # Clean up
            client.close()
            cluster.close()

        # Show monitoring commands
        demonstrate_job_monitoring()

        print(f"\n✅ HPC demonstration completed!")

    except Exception as e:
        print(f"\n❌ HPC demonstration failed: {e}")
        if args.verbose:
            import traceback

            traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
