#!/usr/bin/env python3
# filepath: /work/cwinter/embedding/PrivateTauEmbeddingProduction/condor_job_ressource_usage_analysis.py

import re
import argparse
import sys
from pathlib import Path
from typing import Dict, List
import pandas as pd
from datetime import datetime, timedelta




def parse_log_file(log_file_path: Path) -> Dict:
    """Parse a single HTCondor log file and extract resource usage."""
    # Regex patterns for extracting information from HTCondor logs
    patterns = {
        'cpu_efficiency': re.compile(r'Usr (\d+) (\d+):(\d+):(\d+), Sys (\d+) (\d+):(\d+):(\d+).*?Remote.*?Usr \d+ \d+:\d+:\d+, Sys \d+ \d+:\d+:\d+', re.DOTALL),
        'cpu_usage': re.compile(r'^\t   Cpus\s*:\s*(\S+)\s*(\S+)', re.MULTILINE),
        'memory_usage': re.compile(r'^\t   Memory \(MB\)\s*:\s*(\d+)\s*(\d+)', re.MULTILINE),
        'disk_usage': re.compile(r'^\t   Disk \(KB\)\s*:\s*(\d+)\s*(\d+)', re.MULTILINE),
        'job_start': re.compile(r'(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2}) Job submitted from host'),
        'job_stopped': re.compile(r'(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2}) Job terminated\.'),
        'exit_code': re.compile(r'return value (\d+)'),
        'job_id_from_filename': re.compile(r'Log_(\d+)\.txt'),
    }
    
    try:
        with open(log_file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        # Extract job ID from filename
        job_id_match = patterns['job_id_from_filename'].search(log_file_path.name)
        job_id = job_id_match.group(1) if job_id_match else log_file_path.stem
        
        usage = {
            'job_id': job_id,
            'cpu_efficiency': None,
            'cpu_time': None,
            'cpu_usage': None,
            'cpu_requested': None,
            'memory_used_mb': None,
            'memory_requested_mb': None,
            'disk_used_mb': None,
            'disk_requested_mb': None,
            'runtime_seconds': None,
            'exit_code': None,
            'start_time': None,
            'end_time':None,
        }
        # Extract exit code
        exit_match = patterns['exit_code'].search(content)
        if exit_match:
            usage["exit_code"] = int(exit_match.group(1))
        
        # Only process if job actually terminated normally
        if usage["exit_code"] != 0:
            print(f"Warning: Job {job_id} did not terminate normally (exit code: {usage['exit_code']}), skipping...")
            return None
        
        # Extract CPU efficiency
        cpu_match = patterns['cpu_efficiency'].search(content)
        if cpu_match:
            total_cpu_time = sum((
                int(cpu_match.group(1)) * 86400, # remote user days in sec
                int(cpu_match.group(2)) * 3600,  # remote user hours in sec
                int(cpu_match.group(3)) * 60,    # remote user minutes in sec
                int(cpu_match.group(4)),         # remote user seconds in sec
                int(cpu_match.group(5)) * 86400, # remote sys days in sec
                int(cpu_match.group(6)) * 3600,  # remote sys hours in sec
                int(cpu_match.group(7)) * 60,    # remote sys minutes in sec
                int(cpu_match.group(8))           # remote sys seconds in sec   
            ))

            usage["cpu_time"] = total_cpu_time
        
        # Extract CPU usage
        cpu_usage_match = patterns['cpu_usage'].search(content)
        if cpu_usage_match:
            usage['cpu_usage'] = float(cpu_usage_match.group(1))
            usage['cpu_requested'] = int(cpu_usage_match.group(2))

        # Extract memory usage
        memory_match = patterns['memory_usage'].search(content)
        if memory_match:
            usage['memory_used_mb'] = float(memory_match.group(1))
            usage['memory_requested_mb'] = float(memory_match.group(2))
        
        # Extract disk usage
        disk_match = patterns['disk_usage'].search(content)
        if disk_match:
            usage['disk_used_mb'] = float(disk_match.group(1)) / 1024
            usage['disk_requested_mb'] = float(disk_match.group(2)) / 1024
        
        # Extract start and end times
        job_start_match = patterns['job_start'].search(content)
        job_stopped_match = patterns['job_stopped'].search(content)
        
        if job_start_match:
            usage['start_time'] = datetime(
                datetime.now().year,            # year
                int(job_start_match.group(1)),  # month
                int(job_start_match.group(2)),  # day
                int(job_start_match.group(3)),  # hour
                int(job_start_match.group(4)),  # minute
                int(job_start_match.group(5))   # second
            )
        if job_stopped_match:
            usage['end_time'] = datetime(
                datetime.now().year,              # year
                int(job_stopped_match.group(1)),  # month
                int(job_stopped_match.group(2)),  # day
                int(job_stopped_match.group(3)),  # hour
                int(job_stopped_match.group(4)),  # minute
                int(job_stopped_match.group(5))   # second
            )
        # Calculate runtime from timestamps
        if usage['start_time'] and usage['end_time']:
            runtime_delta = usage['end_time'] - usage['start_time']
            usage['runtime_seconds'] = runtime_delta.total_seconds()
        
        # Calculate CPU efficiency
        if usage['runtime_seconds'] and usage['cpu_time'] and usage['cpu_requested']:
            total_available_cpu_time = usage['runtime_seconds'] * usage['cpu_requested']
            usage['cpu_efficiency'] = (usage['cpu_time'] / total_available_cpu_time) * 100
        return usage
        
    except Exception as e:
        print(f"Error parsing {log_file_path}: {e}")
        return None

def analyze_directory(log_directory: str, file_pattern: str = "Log_*.txt") -> pd.DataFrame:
    """Analyze all log files in the directory."""
    log_files = list(Path(log_directory).glob(file_pattern))
    
    if not log_files:
        print(f"No HTCondor log files ({file_pattern}) found in {log_directory}")
        return
    
    print(f"Found {len(log_files)} log files to analyze...")
    
    data = []
    for log_file in log_files:
        usage = parse_log_file(log_file)
        if usage:
            data.append(usage)
    print(f"Successfully parsed {len(data)} job logs")
    
    # Convert to pandas DataFrame        
    df = pd.DataFrame(data)
    
    # Convert datetime columns
    df['start_time'] = pd.to_datetime(df['start_time'])
    df['end_time'] = pd.to_datetime(df['end_time'])
    return df

def format_time(seconds: float) -> str:
    """Format seconds into human-readable time."""
    if pd.isna(seconds):
        return "N/A"
    
    td = timedelta(seconds=seconds)
    days = td.days
    hours, remainder = divmod(td.seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    
    if days > 0:
        return f"{days}d {hours:02d}:{minutes:02d}:{secs:02d}"
    else:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"

def format_size(size_mb: float) -> str:
    """Format size in MB to human-readable format."""
    if pd.isna(size_mb):
        return "N/A"
    
    if size_mb >= 1024:
        return f"{size_mb/1024:.1f} GB"
    else:
        return f"{size_mb:.1f} MB"

def print_report(df: pd.DataFrame) -> None:
    """Print a comprehensive resource usage report."""
    if df is None or len(df) == 0:
        print("No job data to analyze!")
        return
    
    print("=" * 80)
    
    # Success rate analysis
    print(f"Successful jobs: {(df['exit_code'] == 0).sum()}/{len(df)}")
    print(f"Running from {df['start_time'].min()} to {df['end_time'].max()}\n")

    def print_table_row(values: List[str]):
        print(f"{values[0]:<15} | {values[1]:>10} | {values[2]:>10} | {values[3]:>10} | {values[4]:>10} | {values[5]:>10} | {values[6]:<65}")

    print_table_row(["Statistic", "Min", "Mean", "Max", "Std Dev", "Requested", "Recommended"])
    print("-" * 150)
    
    if (df['cpu_efficiency'].mean() < 70 and df['runtime_seconds'].mean() < 1800) or df['cpu_efficiency'].mean() < 50:
        cpu_reco = "❗ Low CPU efficiency: Reduce CPU cores or optimize code."
    elif df['cpu_efficiency'].mean() > 95 and df['runtime_seconds'].mean() > 3600:
        cpu_reco = "❗ High CPU efficiency: Consider increasing CPU cores if available."
    else:
        cpu_reco = "✅"
    
    print_table_row([
        "CPU Eff (%)",
        f"{df['cpu_efficiency'].min():.1f}",
        f"{df['cpu_efficiency'].mean():.1f}",
        f"{df['cpu_efficiency'].max():.1f}",
        f"{df['cpu_efficiency'].std():.1f}",
        str(df['cpu_requested'].iloc[0]),
        cpu_reco
    ])
    
    mem_usage_ratio = df['memory_used_mb'].max() / df['memory_requested_mb'].iloc[0]
    mem_recommendation = format_size(df['memory_used_mb'].max() * 1.1 / df['cpu_requested'].iloc[0])  # 10% buffer
    if mem_usage_ratio < 0.7:
        mem_reco = f"❗ Memory over-allocated. Recommend: {mem_recommendation} per CPU core."
    elif mem_usage_ratio > 0.9:
        mem_reco = f"❗ Memory usage is high. Recommend: {mem_recommendation} per CPU core."
    else:
        mem_reco = f"✅"
    print_table_row([
        "Mem Used",
        format_size(df['memory_used_mb'].min()),
        format_size(df['memory_used_mb'].mean()),
        format_size(df['memory_used_mb'].max()),
        format_size(df['memory_used_mb'].std()),
        str(format_size(df['memory_requested_mb'].iloc[0])),
        mem_reco
    ])
    
    disk_usage_ratio = df['disk_used_mb'].max() / df['disk_requested_mb'].iloc[0]
    disk_recommendation = format_size(df['disk_used_mb'].max() * 1.1)  # Add 10% buffer to max usage
    if disk_usage_ratio < 0.5:
        disk_reco = f"❗ Disk over-allocated. Recommend: {disk_recommendation}"
    elif disk_usage_ratio > 0.9:
        disk_reco = f"❗ Disk usage is high. Recommend: {disk_recommendation}"
    else:
        disk_reco = f"✅"
    print_table_row([
        "Disk Used",
        format_size(df['disk_used_mb'].min()),
        format_size(df['disk_used_mb'].mean()),
        format_size(df['disk_used_mb'].max()),
        format_size(df['disk_used_mb'].std()),
        str(format_size(df['disk_requested_mb'].iloc[0])),
        disk_reco
    ])
    
    print_table_row([
        "Runtime",
        format_time(df['runtime_seconds'].min()),
        format_time(df['runtime_seconds'].mean()),
        format_time(df['runtime_seconds'].max()),
        format_time(df['runtime_seconds'].std()),
        "",
        f"{df['runtime_seconds'].max()*1.1:.1f} ({format_time(df['runtime_seconds'].max()*1.1)})"
    ])
    


def main():
    """Main function to run the HTCondor log analyzer."""
    parser = argparse.ArgumentParser(
        description="Analyze HTCondor job log files and provide resource usage recommendations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python condor_job_ressource_usage_analysis.py /path/to/jobs/TaskName_hash/logs 'Log_*.txt'
  python condor_job_ressource_usage_analysis.py ./jobs/LawCondorTask__1__False_c8556adb9c/logs
        """
    )
    
    parser.add_argument(
        'log_directory',
        type=str,
        help='Path to directory containing HTCondor log files with file_pattern'
    )
    
    parser.add_argument(
        'file_pattern',
        type=str,
        default='Log_*.txt',
        nargs='?',
        help='File pattern to match HTCondor log files (default: Log_*.txt)'
    )
    
    args = parser.parse_args()
    
    # Validate log directory
    log_dir = Path(args.log_directory)
    if not log_dir.exists() and not log_dir.is_dir():
        print(f"Error: Directory {log_dir} does not exist!")
        sys.exit(1)
    
    # Initialize and run analyzer
    
    df = analyze_directory(args.log_directory, args.file_pattern)
    print_report(df)


if __name__ == "__main__":
    main()