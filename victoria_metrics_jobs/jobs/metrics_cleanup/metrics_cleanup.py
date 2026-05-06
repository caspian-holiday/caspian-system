#!/usr/bin/env python3
"""
Metrics Cleanup Job - Removes old notebook files.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, List, Optional
import os

from ..common import BaseJob, BaseJobState, Err, Ok, Result
from ...scheduler.notebooks_file_manager import NotebooksFileManager


@dataclass
class MetricsCleanupState(BaseJobState):
    """State object for the cleanup job (notebooks only)."""
    
    notebooks_output_dir: Optional[str] = None
    notebooks_retention_days: int = 14  # Retention days for notebooks (from referenced job)
    notebooks_archive_dir: Optional[str] = None
    enable_archive: bool = True
    notebook_dirs_removed: int = 0
    notebooks_cleanup_targets: List[Dict[str, Any]] = field(default_factory=list)


class MetricsCleanupJob(BaseJob):
    """Job that cleans up old notebook output files."""
    
    def __init__(self, config_path: str = None, verbose: bool = False):
        """Initialize the metrics cleanup job.
        
        Args:
            config_path: Path to configuration file
            verbose: Whether to enable verbose logging
        """
        super().__init__('metrics_cleanup', config_path, verbose)
    
    def create_initial_state(self, job_id: str) -> Result[MetricsCleanupState, Exception]:
        """Create initial state for metrics cleanup job.
        
        Args:
            job_id: Job ID to use for configuration selection
            
        Returns:
            Result containing the initial state or an error
        """
        try:
            job_config = self.get_job_config(job_id)
            
            # Get metrics configuration from environment metrics config (added by JobConfigManager)
            metrics_config = job_config.get('metrics', {})
            
            # Get notebooks cleanup settings from referenced job (optional)
            notebooks_output_dir = None
            notebooks_retention_days = metrics_config.get('retention_days', 14)  # Default: 2 weeks
            notebooks_job_id = job_config.get('notebooks_job_id')
            notebooks_cleanup_targets: List[Dict[str, Any]] = []
            
            if notebooks_job_id:
                try:
                    # Get notebooks configuration from the referenced job
                    notebooks_job_config = self.get_job_config(notebooks_job_id)
                    notebooks_output_dir = notebooks_job_config.get('notebooks_output_directory')
                    notebooks_retention_days = notebooks_job_config.get('notebooks_retention_days', notebooks_retention_days)
                    if notebooks_output_dir:
                        notebooks_cleanup_targets.append({
                            "job_id": notebooks_job_id,
                            "notebooks_output_dir": notebooks_output_dir,
                            "notebooks_retention_days": notebooks_retention_days,
                        })
                except Exception as e:
                    self.logger.warning(f"Could not load notebooks job config for '{notebooks_job_id}': {e}")

            notebooks_cleanup_targets = self._discover_all_notebook_cleanup_targets(
                fallback_retention=notebooks_retention_days
            ) or notebooks_cleanup_targets
            
            # Get archive settings from metrics config
            notebooks_archive_dir = metrics_config.get('notebooks_archive_directory')
            enable_archive = metrics_config.get('enable_archive', True)
            
            # Job config can override (for backward compatibility)
            if 'notebooks_output_directory' in job_config:
                notebooks_output_dir = job_config['notebooks_output_directory']
            if 'notebooks_archive_directory' in job_config:
                notebooks_archive_dir = job_config['notebooks_archive_directory']
            if 'enable_archive' in job_config:
                enable_archive = job_config['enable_archive']
            if 'max_age_days' in job_config:
                notebooks_retention_days = job_config['max_age_days']
            
            state = MetricsCleanupState(
                job_id=job_id,
                job_config=job_config,
                started_at=datetime.now(),
                notebooks_output_dir=notebooks_output_dir,
                notebooks_retention_days=notebooks_retention_days,
                notebooks_archive_dir=notebooks_archive_dir,
                enable_archive=enable_archive,
                notebook_dirs_removed=0,
                notebooks_cleanup_targets=notebooks_cleanup_targets
            )
            
            return Ok(state)
            
        except Exception as e:
            return Err(e)
    
    def get_workflow_steps(self) -> List:
        """Get the list of workflow steps for the cleanup job.
        
        Returns:
            List of step functions
        """
        return [
            self._cleanup_notebooks_files,
        ]
    
    def finalize_state(self, state: MetricsCleanupState) -> MetricsCleanupState:
        """Finalize the cleanup state before converting to results.
        
        Args:
            state: The final state after all steps
            
        Returns:
            Finalized state ready for conversion to results
        """
        state.completed_at = datetime.now()
        
        messages = []
        if state.notebook_dirs_removed > 0:
            messages.append(f"Cleaned up {state.notebook_dirs_removed} old notebook directories")
        
        if messages:
            state.status = 'success'
            state.message = "; ".join(messages)
        else:
            state.status = 'success'
            state.message = f"No old notebooks to clean up (checked files older than {state.notebooks_retention_days} days)"
        
        return state
    
    def _cleanup_notebooks_files(self, state: MetricsCleanupState) -> Result[MetricsCleanupState, Exception]:
        """Cleanup old notebooks output files.
        
        Args:
            state: Current job state
            
        Returns:
            Result containing updated state or an error
        """
        targets = state.notebooks_cleanup_targets or []
        if not targets and state.notebooks_output_dir:
            targets = [{
                "job_id": state.job_id,
                "notebooks_output_dir": state.notebooks_output_dir,
                "notebooks_retention_days": state.notebooks_retention_days,
            }]
        if not targets:
            self.logger.info("Notebooks cleanup skipped: no notebooks_output_directory configured")
            return Ok(state)
        
        try:
            total_removed = 0
            for target in targets:
                notebooks_retention = int(target.get("notebooks_retention_days", state.notebooks_retention_days))
                notebooks_base_dir = target.get("notebooks_output_dir")
                notebooks_job_id = target.get("job_id")
                if not notebooks_base_dir or not notebooks_job_id:
                    continue
                notebooks_manager = NotebooksFileManager(
                    notebooks_dir=f"{notebooks_base_dir}/{notebooks_job_id}",
                    archive_dir=state.notebooks_archive_dir,
                    enable_archive=state.enable_archive
                )
                removed = notebooks_manager.cleanup_very_old_files(notebooks_retention)
                total_removed += removed
                self.logger.info(
                    "Notebooks cleanup for %s completed: %s directories removed (retention: %s days)",
                    notebooks_job_id,
                    removed,
                    notebooks_retention,
                )
            state.notebook_dirs_removed = total_removed
            return Ok(state)
            
        except Exception as e:
            self.logger.warning(f"Notebooks cleanup failed (continuing): {e}")
            # Don't fail the entire job if notebooks cleanup fails
            return Ok(state)

    def _discover_all_notebook_cleanup_targets(self, fallback_retention: int) -> List[Dict[str, Any]]:
        environment = os.getenv("VM_JOBS_ENVIRONMENT")
        if not environment:
            return []
        env_cfg = self.config_manager.config.get("environments", {}).get(environment, {})
        jobs_cfg = env_cfg.get("jobs", {})
        targets: List[Dict[str, Any]] = []
        if isinstance(jobs_cfg, dict):
            iterable = jobs_cfg.items()
        else:
            iterable = ((job.get("id"), job) for job in jobs_cfg if isinstance(job, dict))
        for job_id, cfg in iterable:
            if not job_id or not isinstance(cfg, dict):
                continue
            if not cfg.get("enabled", True):
                continue
            if not str(cfg.get("job_type", "")).endswith("_notebooks"):
                continue
            out_dir = cfg.get("notebooks_output_directory")
            if not out_dir:
                continue
            targets.append(
                {
                    "job_id": job_id,
                    "notebooks_output_dir": out_dir,
                    "notebooks_retention_days": cfg.get("notebooks_retention_days", fallback_retention),
                }
            )
        return targets
    
    @classmethod
    def main(cls, description: str = "Metrics Cleanup Job", epilog: str = None):
        """Main entry point for running the metrics cleanup job.
        
        Args:
            description: Description for argument parser
            epilog: Optional epilog text with examples
        """
        parser = cls.create_argument_parser(description, epilog)
        args = parser.parse_args()
        
        if args.list_jobs:
            job = cls(args.config, args.verbose)
            jobs = job.list_jobs()
            print(f"Available job IDs: {', '.join(jobs)}")
            return
        
        if not args.job_id:
            parser.error("--job-id is required when not using --list-jobs")
        
        job = cls(args.config, args.verbose)
        results = job.run_job(args.job_id)
        
        # Output results as JSON
        import json
        print(json.dumps(results, indent=2))
    

