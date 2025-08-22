"""
CAO Extraction Performance Monitoring Module
===========================================

DESCRIPTION:
This module provides comprehensive performance monitoring for the CAO extraction process.
It tracks processing time, token usage, costs, and provides real-time analysis for
large-scale document processing (1,580+ PDFs).

FEATURES:
- Structured JSON logging for all extraction attempts
- Real-time performance summaries
- Cost tracking and analysis
- Error tracking and analysis
- Progress monitoring
- Performance insights and optimization recommendations

USAGE:
    # Import and initialize
    from 3_1_Monitoring import PerformanceMonitor
    
    # Create monitor instance
    monitor = PerformanceMonitor()
    
    # Track extraction
    monitor.log_extraction(
        filename="document.pdf",
        file_size_mb=5.2,
        processing_time=120.5,
        usage_metadata=response.usage_metadata,
        success=True
    )
    
    # Get real-time summary
    monitor.print_summary()
    
    # Analyze performance
    monitor.analyze_performance()
"""

import json
import time
import fcntl
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List
from google.genai import types

class PerformanceMonitor:
    """Comprehensive performance monitoring for CAO extraction"""
    
    def __init__(self, 
                 log_file: str = "extraction_performance.jsonl",
                 summary_file: str = "extraction_summary.json",
                 free_tier_daily_limit: int = 100):
        """
        Initialize performance monitor
        
        Args:
            log_file: JSON Lines file for detailed logging
            summary_file: JSON file for summary statistics
            free_tier_daily_limit: Daily request limit for free tier (default: 100)
        """
        self.log_file = log_file
        self.summary_file = summary_file
        self.free_tier_daily_limit = free_tier_daily_limit
        
        # Ensure log directory exists
        Path(self.log_file).parent.mkdir(exist_ok=True)
        Path(self.summary_file).parent.mkdir(exist_ok=True)
    
    def log_extraction(self,
                      filename: str,
                      file_size_mb: float,
                      processing_time: float,
                      usage_metadata: Optional[types.UsageMetadata],
                      success: bool,
                      error_message: Optional[str] = None,
                      api_key_used: int = 1,
                      process_id: int = 0,
                      cao_number: str = "",
                      allow_duplicates: bool = False) -> None:
        """
        Log detailed performance data for a single extraction
        
        Args:
            filename: Name of the processed file
            file_size_mb: File size in megabytes
            processing_time: Processing time in seconds
            usage_metadata: Gemini API usage metadata
            success: Whether extraction was successful
            error_message: Error message if failed
            api_key_used: API key number used
            process_id: Process ID for multi-processing
            cao_number: CAO number for the file (to distinguish same filenames in different folders)
            allow_duplicates: If False, check for existing entries first
        """
        
        # Check for duplicate entries if not allowed
        if not allow_duplicates:
            existing_data = self.get_performance_data()
            for i, entry in enumerate(existing_data):
                # Check for same file in same CAO (regardless of API key or success status)
                # This means any re-run of the same file should replace the previous entry
                if (entry.get("filename") == filename and 
                    entry.get("cao_number", "") == cao_number):
                    # File already processed - replace the old entry with new one
                    # We'll remove the old entry and let the new one be logged
                    existing_data.pop(i)
                    # Rewrite the log file without the old entry
                    self._rewrite_log_file(existing_data)
                    break
        
        # Calculate token usage (free tier - no cost)
        input_tokens = usage_metadata.prompt_token_count if usage_metadata else 0
        output_tokens = usage_metadata.candidates_token_count if usage_metadata else 0
        total_tokens = usage_metadata.total_token_count if usage_metadata else 0
        
        # Create performance data record
        performance_data = {
            "timestamp": datetime.now().isoformat(),
            "filename": filename,
            "cao_number": cao_number,  # Add CAO number for proper deduplication
            "file_size_mb": round(file_size_mb, 2),
            "processing_time_seconds": round(processing_time, 2),
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": total_tokens,
            "success": success,
            "error_message": error_message,
            "api_key_used": api_key_used,
            "process_id": process_id,
            "free_tier_request": True  # All requests are free tier
        }
        
        # Write to JSON Lines file (one JSON object per line)
        with open(self.log_file, "a", encoding="utf-8") as f:
            # Acquire exclusive lock to prevent race conditions
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            try:
                f.write(json.dumps(performance_data) + "\n")
                f.flush()  # Ensure data is written immediately
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    
    def _rewrite_log_file(self, data: List[Dict[str, Any]]) -> None:
        """Rewrite the entire log file with new data (used when replacing entries)"""
        with open(self.log_file, "w", encoding="utf-8") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            try:
                for entry in data:
                    f.write(json.dumps(entry) + "\n")
                f.flush()
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    
    def get_performance_data(self) -> List[Dict[str, Any]]:
        """Load all performance data from log file"""
        try:
            with open(self.log_file, "r", encoding="utf-8") as f:
                lines = f.readlines()
            
            if not lines:
                return []
            
            return [json.loads(line) for line in lines if line.strip()]
        except FileNotFoundError:
            return []
    
    def calculate_summary(self) -> Dict[str, Any]:
        """Calculate comprehensive performance summary"""
        data = self.get_performance_data()
        
        if not data:
            return {
                "total_files_processed": 0,
                "successful_extractions": 0,
                "failed_extractions": 0,
                "total_processing_time_hours": 0,
                "total_tokens_used": 0,
                "total_requests_used": 0,
                "avg_processing_time_seconds": 0,
                "avg_tokens_per_file": 0,
                "largest_file_mb": 0,
                "slowest_file_seconds": 0,
                "daily_request_limit": self.free_tier_daily_limit,
                "last_updated": datetime.now().isoformat()
            }
        
        successful = [d for d in data if d["success"]]
        failed = [d for d in data if not d["success"]]
        
        summary = {
            "total_files_processed": len(data),
            "successful_extractions": len(successful),
            "failed_extractions": len(failed),
            "total_processing_time_hours": sum(d["processing_time_seconds"] for d in data) / 3600,
            "total_tokens_used": sum(d["total_tokens"] for d in data),
            "total_requests_used": len(data),  # Each file = 1 request
            "avg_processing_time_seconds": sum(d["processing_time_seconds"] for d in data) / len(data),
            "avg_tokens_per_file": sum(d["total_tokens"] for d in data) / len(data),
            "largest_file_mb": max(d["file_size_mb"] for d in data),
            "slowest_file_seconds": max(d["processing_time_seconds"] for d in data),
            "most_tokens_used": max(d["total_tokens"] for d in data),
            "success_rate_percent": (len(successful) / len(data)) * 100 if data else 0,
            "daily_request_limit": self.free_tier_daily_limit,
            "requests_remaining_today": max(0, self.free_tier_daily_limit - len(data)),
            "last_updated": datetime.now().isoformat()
        }
        
        return summary
    
    def update_summary_file(self) -> None:
        """Update the summary JSON file"""
        summary = self.calculate_summary()
        
        with open(self.summary_file, "w", encoding="utf-8") as f:
            # Acquire exclusive lock to prevent race conditions
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            try:
                json.dump(summary, f, indent=2)
                f.flush()  # Ensure data is written immediately
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    
    def print_summary(self) -> None:
        """Print current performance summary to console"""
        summary = self.calculate_summary()
        
        print(f"\n📊 PERFORMANCE SUMMARY:")
        print(f"   Files processed: {summary['successful_extractions']}/{summary['total_files_processed']} "
              f"({summary['success_rate_percent']:.1f}% success)")
        print(f"   Requests used: {summary['total_requests_used']}/{summary['daily_request_limit']} "
              f"({summary['requests_remaining_today']} remaining today)")
        print(f"   Total time: {summary['total_processing_time_hours']:.1f} hours")
        print(f"   Total tokens: {summary['total_tokens_used']:,}")
        print(f"   Avg time/file: {summary['avg_processing_time_seconds']:.1f}s")
        print(f"   Avg tokens/file: {summary['avg_tokens_per_file']:.0f}")
        
        if summary['failed_extractions'] > 0:
            print(f"   ⚠️  Failed extractions: {summary['failed_extractions']}")
        
        if summary['requests_remaining_today'] == 0:
            print(f"   ⚠️  WARNING: Daily request limit reached! Wait until tomorrow to continue.")
    
    def analyze_performance(self) -> None:
        """Analyze performance data for insights and optimization"""
        data = self.get_performance_data()
        
        if not data:
            print("No performance data available for analysis.")
            return
        
        successful = [d for d in data if d["success"]]
        failed = [d for d in data if not d["success"]]
        
        print(f"\n🔍 PERFORMANCE ANALYSIS:")
        
        # Request usage analysis
        total_requests = len(data)
        print(f"\n📊 REQUEST USAGE ANALYSIS:")
        print(f"   Total requests: {total_requests}/{self.free_tier_daily_limit}")
        print(f"   Requests remaining today: {max(0, self.free_tier_daily_limit - total_requests)}")
        print(f"   Success rate: {len(successful)}/{total_requests} ({(len(successful)/total_requests)*100:.1f}%)" if total_requests > 0 else "   No requests made")
        
        # Token usage by file size
        tokens_by_size = {}
        for d in data:
            size_bucket = f"{int(d['file_size_mb'])}-{int(d['file_size_mb'])+1}MB"
            if size_bucket not in tokens_by_size:
                tokens_by_size[size_bucket] = []
            tokens_by_size[size_bucket].append(d["total_tokens"])
        
        print(f"   Token usage by file size:")
        for bucket, tokens in sorted(tokens_by_size.items()):
            avg_tokens = sum(tokens) / len(tokens)
            print(f"     {bucket}: {avg_tokens:,.0f} tokens avg ({len(tokens)} files)")
        
        # Performance analysis
        print(f"\n⚡ PERFORMANCE INSIGHTS:")
        
        # Slowest files
        slowest_files = sorted(data, key=lambda x: x["processing_time_seconds"], reverse=True)[:5]
        print(f"   Slowest files:")
        for file_data in slowest_files:
            print(f"     {file_data['filename']}: {file_data['processing_time_seconds']:.1f}s "
                  f"({file_data['file_size_mb']:.1f}MB)")
        
        # Most token-intensive files
        most_tokens = sorted(data, key=lambda x: x["total_tokens"], reverse=True)[:5]
        print(f"   Most token-intensive files:")
        for file_data in most_tokens:
            print(f"     {file_data['filename']}: {file_data['total_tokens']:,} tokens "
                  f"({file_data['file_size_mb']:.1f}MB)")
        
        # Error analysis
        if failed:
            print(f"\n❌ ERROR ANALYSIS:")
            error_types = {}
            for file_data in failed:
                error_msg = file_data.get("error_message", "Unknown error")
                error_type = error_msg.split(":")[0] if ":" in error_msg else "Unknown"
                error_types[error_type] = error_types.get(error_type, 0) + 1
            
            for error_type, count in sorted(error_types.items(), key=lambda x: x[1], reverse=True):
                print(f"     {error_type}: {count} occurrences")
    
    def get_progress_estimate(self, total_files: int) -> Dict[str, Any]:
        """Estimate progress and remaining time with request limits"""
        summary = self.calculate_summary()
        
        if summary["total_files_processed"] == 0:
            return {
                "progress_percent": 0,
                "remaining_files": total_files,
                "estimated_remaining_time_hours": 0,
                "requests_needed": total_files,
                "days_needed_at_current_rate": (total_files / self.free_tier_daily_limit)
            }
        
        progress_percent = (summary["total_files_processed"] / total_files) * 100
        remaining_files = total_files - summary["total_files_processed"]
        
        # Calculate averages from successful extractions
        successful = [d for d in self.get_performance_data() if d["success"]]
        if successful:
            avg_time = sum(d["processing_time_seconds"] for d in successful) / len(successful)
        else:
            avg_time = 0
        
        estimated_remaining_time_hours = (remaining_files * avg_time) / 3600
        days_needed = remaining_files / self.free_tier_daily_limit
        
        return {
            "progress_percent": progress_percent,
            "remaining_files": remaining_files,
            "estimated_remaining_time_hours": estimated_remaining_time_hours,
            "requests_needed": remaining_files,
            "days_needed_at_current_rate": days_needed,
            "avg_time_per_file_seconds": avg_time,
            "requests_remaining_today": summary["requests_remaining_today"]
        }
    
    def print_progress(self, total_files: int) -> None:
        """Print progress estimate"""
        progress = self.get_progress_estimate(total_files)
        
        print(f"\n📈 PROGRESS ESTIMATE:")
        print(f"   Progress: {progress['progress_percent']:.1f}% "
              f"({progress['remaining_files']} files remaining)")
        print(f"   Estimated remaining time: {progress['estimated_remaining_time_hours']:.1f} hours")
        print(f"   Requests needed: {progress['requests_needed']:,}")
        print(f"   Days needed at 100 req/day: {progress['days_needed_at_current_rate']:.1f}")
        print(f"   Requests remaining today: {progress['requests_remaining_today']}")
        print(f"   Average time per file: {progress['avg_time_per_file_seconds']:.1f}s")

# Convenience functions for easy integration
def create_monitor() -> PerformanceMonitor:
    """Create a default performance monitor instance"""
    return PerformanceMonitor()

def log_extraction(monitor: PerformanceMonitor,
                  filename: str,
                  file_size_mb: float,
                  processing_time: float,
                  usage_metadata: Optional[types.UsageMetadata],
                  success: bool,
                  error_message: Optional[str] = None,
                  api_key_used: int = 1,
                  process_id: int = 0) -> None:
    """Convenience function to log extraction with monitor"""
    monitor.log_extraction(
        filename=filename,
        file_size_mb=file_size_mb,
        processing_time=processing_time,
        usage_metadata=usage_metadata,
        success=success,
        error_message=error_message,
        api_key_used=api_key_used,
        process_id=process_id
    )
    monitor.update_summary_file()
    monitor.print_summary()
