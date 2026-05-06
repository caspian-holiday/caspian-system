#!/usr/bin/env python3
"""
Main Victoria Metrics Jobs service implementation using APScheduler.
"""

import logging
import os
import platform
import signal
import sys
import time
import inspect
from typing import Dict, Any, Optional
from threading import Thread, current_thread, main_thread

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from apscheduler.schedulers.base import BaseScheduler
from flask import Flask, request, redirect
from waitress import serve as waitress_serve

try:
    from gunicorn.app.base import BaseApplication
except Exception:
    BaseApplication = None

from .config import ConfigLoader
from .jobs import JobExecutor
from .database import DatabaseManager
from .notebooks_file_manager import NotebooksFileManager
from .sidecar_service import SidecarService

from ..version import get_app_version


class EmbeddedGunicornApplication(BaseApplication if BaseApplication is not None else object):
    """Embedded Gunicorn runner for in-process HTTP serving."""

    def __init__(self, app, options=None):
        self.options = options or {}
        self.application = app
        super().__init__()

    def load_config(self):
        valid_options = {
            key: value
            for key, value in self.options.items()
            if key in self.cfg.settings and value is not None
        }
        for key, value in valid_options.items():
            self.cfg.set(key.lower(), value)

    def load(self):
        return self.application


class SchedulerService:
    """Main Victoria Metrics Jobs service that manages job execution."""
    
    def __init__(self, config_path: str):
        """Initialize the Victoria Metrics Jobs service.
        
        Args:
            config_path: Path to the YAML configuration file (required)
        """
        self.config_path = config_path
        self.scheduler: Optional[BaseScheduler] = None
        self.config_loader = ConfigLoader()
        self.database_manager: Optional[DatabaseManager] = None
        self.job_executor: Optional[JobExecutor] = None
        self.notebooks_manager: Optional[NotebooksFileManager] = None
        self.http_app: Optional[Flask] = None
        self.sidecar_app: Optional[Flask] = None
        self.http_thread: Optional[Thread] = None
        self.sidecar_http_thread: Optional[Thread] = None
        self.notebooks_config: Optional[Dict[str, Any]] = None
        self.notebook_jobs: Dict[str, Dict[str, Any]] = {}
        self.runtime_config: Dict[str, Any] = {}
        self.sidecar_service: Optional[SidecarService] = None
        self.logger = logging.getLogger(__name__)
        self.running = False
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        self.logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.stop()
    
    def start(self):
        """Start the scheduler service."""
        try:
            # Validate config file exists
            if not os.path.exists(self.config_path):
                raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
            
            # Load configuration
            config = self.config_loader.load(self.config_path)
            self.runtime_config = config
            self.logger.info(f"Loaded configuration from {self.config_path}")
            
            # Initialize database manager if database config is present
            if 'database' in config:
                try:
                    self.database_manager = DatabaseManager(config['database'])
                    self.database_manager.connect()
                    
                    # Test database connection and advisory locks
                    if self.database_manager.test_connection():
                        self.logger.info("Database connection and advisory locks verified")
                    else:
                        self.logger.error("Database connection test failed")
                        raise RuntimeError("Database connection test failed")
                        
                except Exception as e:
                    self.logger.error(f"Failed to initialize database manager: {e}")
                    raise
            else:
                self.logger.warning("No database configuration found - jobs will run without advisory locking")
            
            # Initialize notebooks manager and HTTP server if notebooks config is present
            if 'metrics' in config:
                metrics_config = config['metrics']
                self.notebook_jobs = self._collect_notebook_jobs(config)
                notebooks_output_dir = metrics_config.get('notebooks_output_directory')
                if not notebooks_output_dir and self.notebook_jobs:
                    first_job = next(iter(self.notebook_jobs.values()))
                    notebooks_output_dir = first_job.get("notebooks_output_directory")
                
                if notebooks_output_dir:
                    try:
                        # Initialize notebooks manager for cleanup and serving
                        self.notebooks_manager = NotebooksFileManager(
                            notebooks_dir=notebooks_output_dir,
                            archive_dir=metrics_config.get('notebooks_archive_directory'),
                            enable_archive=metrics_config.get('enable_archive', True)
                        )
                        self.logger.info(f"Initialized notebooks manager with directory: {notebooks_output_dir}")
                        
                        # Start HTTP server for notebooks and health endpoints
                        http_port = int(metrics_config.get('notebooks_port', metrics_config.get('port', 8000)))
                        http_host = metrics_config.get('notebooks_host', metrics_config.get('host', '0.0.0.0'))
                        self._start_http_servers(http_host, http_port, metrics_config)
                        
                        # Store notebooks config
                        self.notebooks_config = metrics_config
                        
                    except Exception as e:
                        self.logger.warning(f"Failed to initialize notebooks manager: {e}. Notebooks serving will not be available.")
                        self.notebooks_manager = None
                else:
                    # Start HTTP server for health endpoint only
                    http_port = int(metrics_config.get('notebooks_port', metrics_config.get('port', 8000)))
                    http_host = metrics_config.get('notebooks_host', metrics_config.get('host', '0.0.0.0'))
                    self._start_http_servers(http_host, http_port, metrics_config)
            
            # Initialize job executor with database manager and config path
            self.sidecar_service = SidecarService(
                database_manager=self.database_manager,
                runtime_config=self.runtime_config,
            )
            self.job_executor = JobExecutor(
                self.database_manager,
                self.config_path
            )
            
            # Configure scheduler
            jobstores = {
                'default': MemoryJobStore()
            }
            executors = {
                'default': ThreadPoolExecutor(max_workers=config.get('max_workers', 10))
            }
            job_defaults = {
                'coalesce': True,
                'max_instances': 3
            }
            
            self.scheduler = BlockingScheduler(
                jobstores=jobstores,
                executors=executors,
                job_defaults=job_defaults
            )
            
            # Add event listeners
            self.scheduler.add_listener(self._job_executed, EVENT_JOB_EXECUTED)
            self.scheduler.add_listener(self._job_error, EVENT_JOB_ERROR)
            
            # Add jobs from configuration
            self._add_jobs_from_config(config)
            
            self.running = True
            self.logger.info("Scheduler service started successfully")
            
            # Start the scheduler (this will block)
            self.scheduler.start()
            
        except Exception as e:
            self.logger.error(f"Failed to start scheduler service: {e}")
            raise
    
    def stop(self):
        """Stop the scheduler service."""
        if self.scheduler and self.running:
            self.logger.info("Stopping scheduler service...")
            self.scheduler.shutdown(wait=True)
            self.running = False
            self.logger.info("Scheduler service stopped")
        
        # Stop HTTP server
        if self.http_app:
            # Flask doesn't have a clean shutdown, but we can mark it
            self.logger.info("Stopping HTTP server...")
            # The thread will exit when scheduler stops
        
        # Disconnect from database
        if self.database_manager:
            self.database_manager.disconnect()
    
    def _start_http_servers(self, host: str, port: int, config: Dict[str, Any]):
        """Start separate Gunicorn instances for UI and sidecar endpoints.
        
        Args:
            host: Host to bind the server
            port: Port to bind the server
            config: Configuration dictionary (for notebooks directory)
        """
        from pathlib import Path
        
        self.http_app = Flask(__name__)
        self.sidecar_app = Flask(f"{__name__}.sidecar")
        
        @self.http_app.route('/health')
        @self.http_app.route('/vmj/health')
        def health():
            """Health check endpoint."""
            return {'status': 'ok', 'version': get_app_version()}, 200

        @self.sidecar_app.route('/health')
        @self.sidecar_app.route('/vmj/health')
        def sidecar_health():
            """Health check endpoint for sidecar server."""
            return {'status': 'ok', 'version': get_app_version(), 'service': 'sidecar'}, 200
        
        @self.http_app.route('/notebooks')
        @self.http_app.route('/vmj/notebooks')
        def notebooks_jobs_listing():
            """List notebook job routes."""
            if not self.notebooks_manager:
                return {'error': 'Notebooks manager not initialized'}, 503
            html_parts = ["<html><head><title>Notebook Jobs</title></head><body>"]
            html_parts.append("<h1>Notebook Jobs</h1><ul>")
            path_prefix = "/vmj" if request.path.startswith("/vmj/") else ""
            for job_id in sorted(self.notebook_jobs.keys()):
                html_parts.append(f'<li><a href="{path_prefix}/notebooks/{job_id}/">{job_id}</a></li>')
            html_parts.append("</ul></body></html>")
            return "".join(html_parts), 200, {"Content-Type": "text/html"}
        
        @self.http_app.route('/notebooks/<job_id>')
        @self.http_app.route('/notebooks/<job_id>/')
        @self.http_app.route('/vmj/notebooks/<job_id>')
        @self.http_app.route('/vmj/notebooks/<job_id>/')
        def notebooks_listing(job_id):
            """List available notebooks for a single job ID."""
            # Ensure trailing slash so relative links keep job_id in path.
            if not request.path.endswith("/"):
                return redirect(request.path + "/", code=302)
            if not self.notebooks_manager:
                return {'error': 'Notebooks manager not initialized'}, 503
            job_config = self.notebook_jobs.get(job_id)
            if not job_config:
                return {'error': f'Unknown notebook job_id: {job_id}'}, 404
            notebooks_dir = Path(job_config.get("notebooks_output_directory", "")) / job_id
            path_prefix = "/vmj" if request.path.startswith("/vmj/") else ""
            return self._serve_notebook_directory_listing_compat(
                notebooks_dir, route_prefix=f"{path_prefix}/notebooks/{job_id}"
            )

        @self.http_app.route('/notebooks/<job_id>/<year>/<month>/<day>/<filename>')
        @self.http_app.route('/vmj/notebooks/<job_id>/<year>/<month>/<day>/<filename>')
        def notebooks_file(job_id, year, month, day, filename):
            """Serve a notebook artifact for a single job ID."""
            if not self.notebooks_manager:
                return {'error': 'Notebooks manager not initialized'}, 503
            job_config = self.notebook_jobs.get(job_id)
            if not job_config:
                return {'error': f'Unknown notebook job_id: {job_id}'}, 404
            notebooks_dir = Path(job_config.get("notebooks_output_directory", "")) / job_id
            return self.notebooks_manager.serve_notebook_file(notebooks_dir, year, month, day, filename)

        @self.sidecar_app.route('/api/v1/write', methods=['POST'])
        @self.sidecar_app.route('/vmj/api/v1/write', methods=['POST'])
        @self.sidecar_app.route('/vmj/api/v1/write/biz-date', methods=['POST'])
        def sidecar_write():
            if not self.sidecar_service:
                return {"error": "Sidecar service not initialized"}, 503
            return self.sidecar_service.handle_write()
        
        def run_http_server():
            """Run embedded Gunicorn for health/notebooks app."""
            try:
                options = self._build_gunicorn_options(
                    bind_host=host,
                    bind_port=port,
                    config=config,
                    prefix="notebooks_",
                )
                # Embedded Gunicorn relies on signal handlers and is not safe from
                # non-main threads. Fall back to waitress in this execution model.
                if (
                    platform.system().lower().startswith("win")
                    or BaseApplication is None
                    or current_thread() is not main_thread()
                ):
                    waitress_serve(
                        self.http_app,
                        host=host,
                        port=port,
                        threads=int(config.get("notebooks_gunicorn_threads", 8)),
                    )
                else:
                    EmbeddedGunicornApplication(self.http_app, options).run()
            except Exception as e:
                self.logger.error(f"HTTP server error: {e}")

        sidecar_host = config.get("sidecar_host", host)
        sidecar_port = int(config.get("sidecar_port", int(port) + 1))

        def run_sidecar_server():
            """Run embedded Gunicorn for sidecar app."""
            try:
                options = self._build_gunicorn_options(
                    bind_host=sidecar_host,
                    bind_port=sidecar_port,
                    config=config,
                    prefix="sidecar_",
                )
                # Embedded Gunicorn relies on signal handlers and is not safe from
                # non-main threads. Fall back to waitress in this execution model.
                if (
                    platform.system().lower().startswith("win")
                    or BaseApplication is None
                    or current_thread() is not main_thread()
                ):
                    waitress_serve(
                        self.sidecar_app,
                        host=sidecar_host,
                        port=sidecar_port,
                        threads=int(config.get("sidecar_gunicorn_threads", 16)),
                    )
                else:
                    EmbeddedGunicornApplication(self.sidecar_app, options).run()
            except Exception as e:
                self.logger.error(f"Sidecar HTTP server error: {e}")
        
        self.http_thread = Thread(target=run_http_server, daemon=True)
        self.http_thread.start()
        self.sidecar_http_thread = Thread(target=run_sidecar_server, daemon=True)
        self.sidecar_http_thread.start()
        self.logger.info(
            "HTTP server started on %s:%s and sidecar server on %s:%s via embedded gunicorn",
            host,
            port,
            sidecar_host,
            sidecar_port,
        )

    def _build_gunicorn_options(
        self,
        bind_host: str,
        bind_port: int,
        config: Dict[str, Any],
        prefix: str,
    ) -> Dict[str, Any]:
        """Build gunicorn options from metrics config using optional key prefix."""
        def key(name: str) -> str:
            return f"{prefix}{name}"

        return {
            "bind": f"{bind_host}:{bind_port}",
            "workers": int(config.get(key("gunicorn_workers"), 1)),
            "worker_class": config.get(key("gunicorn_worker_class"), "gthread"),
            "threads": int(config.get(key("gunicorn_threads"), 16)),
            "timeout": int(config.get(key("gunicorn_timeout_seconds"), 30)),
            "graceful_timeout": int(config.get(key("gunicorn_graceful_timeout_seconds"), 30)),
            "keepalive": int(config.get(key("gunicorn_keepalive_seconds"), 5)),
            "backlog": int(config.get(key("gunicorn_backlog"), 2048)),
            "max_requests": int(config.get(key("gunicorn_max_requests"), 10000)),
            "max_requests_jitter": int(config.get(key("gunicorn_max_requests_jitter"), 1000)),
            "accesslog": "-",
            "errorlog": "-",
            "capture_output": True,
            "loglevel": "info",
        }

    def _collect_notebook_jobs(self, config: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """Collect notebook-producing jobs from scheduler config."""
        notebook_jobs: Dict[str, Dict[str, Any]] = {}
        jobs = config.get("jobs", {})
        if isinstance(jobs, dict):
            iterable = jobs.items()
        else:
            iterable = ((job.get("id"), job) for job in jobs if isinstance(job, dict))
        for job_id, job_cfg in iterable:
            if not job_id or not isinstance(job_cfg, dict):
                continue
            if not job_cfg.get("enabled", True):
                continue
            if not str(job_cfg.get("job_type", "")).endswith("_notebooks"):
                continue
            notebooks_output = job_cfg.get("notebooks_output_directory")
            if not notebooks_output:
                continue
            notebook_jobs[job_id] = {"notebooks_output_directory": notebooks_output}
        return notebook_jobs

    def _serve_notebook_directory_listing_compat(self, notebooks_dir, route_prefix: str):
        """
        Backward-compatible wrapper for NotebooksFileManager.serve_notebook_directory_listing.

        Older prod versions may not support the route_prefix argument.
        """
        fn = getattr(self.notebooks_manager, "serve_notebook_directory_listing", None)
        if fn is None:
            return {'error': 'Notebooks manager missing directory listing'}, 503
        try:
            sig = inspect.signature(fn)
            if "route_prefix" in sig.parameters:
                return fn(notebooks_dir, route_prefix=route_prefix)
        except Exception:
            # If signature introspection fails, fall back to old call
            pass
        return fn(notebooks_dir)

    def _add_jobs_from_config(self, config: Dict[str, Any]):
        """Add jobs to the scheduler based on configuration."""
        jobs = config.get('jobs', {})
        
        if not jobs:
            self.logger.info("No jobs configured - scheduler will run with no scheduled jobs")
            return
        
        jobs_added = 0
        
        # Handle both dict and list formats for jobs
        if isinstance(jobs, dict):
            # Convert dict format to list for processing
            jobs_list = []
            for job_id, job_config in jobs.items():
                # Ensure job_id is in the config
                if 'id' not in job_config:
                    job_config['id'] = job_id
                jobs_list.append(job_config)
            jobs = jobs_list
        
        for job_config in jobs:
            try:
                job_id = job_config.get('id')
                script = job_config.get('script')
                schedule = job_config.get('schedule')
                enabled = job_config.get('enabled', True)
                
                if not enabled:
                    self.logger.info(f"Skipping disabled job: {job_id}")
                    continue
                
                if not all([job_id, script, schedule]):
                    self.logger.warning(f"Invalid job configuration: {job_config}")
                    continue
                
                # Add job to scheduler
                self.scheduler.add_job(
                    func=self.job_executor.execute_job,
                    trigger=schedule.get('type', 'cron'),
                    args=[job_config],
                    id=job_id,
                    name=job_config.get('name', job_id),
                    **schedule.get('args', {})
                )
                
                # Format schedule in human-readable form
                schedule_str = self._format_schedule(schedule)
                self.logger.info(f"Added job: {job_id} (script: {script}, schedule: {schedule_str})")
                jobs_added += 1
                
            except Exception as e:
                self.logger.error(f"Failed to add job {job_config.get('id', 'unknown')}: {e}")
        
        if jobs_added == 0:
            self.logger.info("No enabled jobs found - scheduler will run with no scheduled jobs")
    
    def _format_schedule(self, schedule: Dict[str, Any]) -> str:
        """Format schedule configuration in human-readable form.
        
        Args:
            schedule: Schedule configuration dictionary
            
        Returns:
            Human-readable schedule description
        """
        schedule_type = schedule.get('type', 'cron')
        args = schedule.get('args', {})
        
        if schedule_type == 'cron':
            try:
                from cron_descriptor import get_description, CasingTypeEnum
                
                # Convert APScheduler keyword args to cron expression: minute hour day month day_of_week
                minute = str(args.get('minute', '*'))
                hour = str(args.get('hour', '*'))
                day = str(args.get('day', '*'))
                month = str(args.get('month', '*'))
                day_of_week = args.get('day_of_week', '*')
                
                # Handle lists/ranges for day_of_week
                if isinstance(day_of_week, (list, tuple)):
                    day_of_week = ','.join(str(d) for d in day_of_week)
                else:
                    day_of_week = str(day_of_week)
                
                cron_expr = f"{minute} {hour} {day} {month} {day_of_week}"
                return get_description(cron_expr, casing_type=CasingTypeEnum.Sentence)
            except ImportError:
                # Fallback if cron-descriptor not available
                hour = args.get('hour', '*')
                minute = args.get('minute', '*')
                return f"daily at {int(hour):02d}:{int(minute):02d}" if hour != '*' and minute != '*' else "daily"
        
        elif schedule_type == 'interval':
            # Simple interval formatting
            parts = []
            for unit in ['weeks', 'days', 'hours', 'minutes', 'seconds']:
                if unit in args:
                    val = args[unit]
                    unit_name = unit.rstrip('s') if val == 1 else unit
                    parts.append(f"{val} {unit_name}")
            return f"every {', '.join(parts)}" if parts else "interval"
        
        elif schedule_type == 'date':
            run_date = args.get('run_date', 'unspecified')
            return f"on {run_date}"
        
        else:
            return f"{schedule_type}"
    
    def _job_executed(self, event):
        """Handle successful job execution."""
        self.logger.info(f"Job {event.job_id} executed successfully")
    
    def _job_error(self, event):
        """Handle job execution errors."""
        self.logger.error(f"Job {event.job_id} failed: {event.exception}")
    
    
    def reload_config(self):
        """Reload configuration and restart scheduler."""
        self.logger.info("Reloading configuration...")
        self.stop()
        time.sleep(1)  # Brief pause before restart
        self.start()
