"""
Knowledge base refresh scheduler.
Handles startup initialization and hourly scheduled refresh.
Implements atomic file swaps with "last known good" fallback.
"""
import asyncio
from datetime import datetime, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from pathlib import Path
import json
import aiofiles
import time
from core.schema_introspector import SchemaIntrospector
from core.semantic_store import semantic_store
from core.rules_compiler import rules_compiler
from observability.logger import get_logger
from observability.metrics import metrics
from core.config import settings

logger = get_logger(__name__)

KB_DIR = Path(__file__).parent.parent / "kb"


class KBRefreshScheduler:
    """Manages KB refresh lifecycle: startup + hourly scheduled refresh."""
    
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self.last_refresh_time: datetime = None
        self.last_refresh_status: str = "never_run"
        self.last_refresh_error: str = None
        self.current_version: str = None
        self.is_refreshing: bool = False
    
    async def refresh_knowledge_base(self) -> dict:
        """
        Complete KB refresh pipeline with atomic swap.
        
        Steps:
        1. Extract metadata → kb_schema_temp.json
        2. Merge semantic → kb_semantic_temp.json
        3. Compile rules → compiled_rules_temp.json
        4. Validate all three files
        5. Atomic swap if valid, otherwise keep "last known good"
        
        Returns:
            dict with status, version, table_count, duration, error
        """
        if self.is_refreshing:
            logger.warning("kb_refresh_already_running")
            return {
                "status": "skipped",
                "reason": "refresh already in progress"
            }
        
        self.is_refreshing = True
        start_time = time.time()
        
        try:
            logger.info("kb_refresh_started")
            
            # Step 1: Extract schema metadata
            introspector = SchemaIntrospector(settings.schema_name)
            kb_schema = await introspector.build_kb_schema()
            
            # Add timestamp
            kb_schema['generated_at'] = datetime.utcnow().isoformat()
            
            # Save to temp file
            async with aiofiles.open(KB_DIR / "kb_schema_temp.json", 'w') as f:
                await f.write(json.dumps(kb_schema, indent=2))
            
            logger.info("kb_schema_generated", table_count=len(kb_schema['tables']))
            
            # Step 2: Merge semantic KB
            merged_semantic = await semantic_store.merge_with_schema(kb_schema)
            await semantic_store.save(merged_semantic, temp=True)
            
            logger.info("kb_semantic_merged", table_count=len(merged_semantic))
            
            # Step 3: Compile rules
            compiled_rules = await rules_compiler.compile_rules()
            
            # Validate compiled rules
            is_valid = await rules_compiler.validate_compiled_rules(compiled_rules)
            
            if not is_valid:
                raise ValueError("Compiled rules validation failed")
            
            # Save to temp file
            await rules_compiler.save_compiled_rules(compiled_rules, temp=True)
            
            # Step 4: Atomic swap
            await rules_compiler.atomic_swap()
            
            # Update state
            duration = time.time() - start_time
            version = compiled_rules['version']
            table_count = len(compiled_rules['tables'])
            
            self.last_refresh_time = datetime.now()
            self.last_refresh_status = "success"
            self.last_refresh_error = None
            self.current_version = version
            
            # Record metrics
            metrics.record_kb_refresh(success=True, version=version)
            
            logger.info(
                "kb_refresh_completed",
                duration_seconds=round(duration, 2),
                version=version,
                table_count=table_count
            )
            
            return {
                "status": "success",
                "version": version,
                "table_count": table_count,
                "duration_seconds": round(duration, 2),
                "timestamp": self.last_refresh_time.isoformat()
            }
            
        except Exception as e:
            duration = time.time() - start_time
            error_msg = str(e)
            
            self.last_refresh_status = "failed"
            self.last_refresh_error = error_msg
            
            # Record metrics
            metrics.record_kb_refresh(success=False)
            
            logger.error(
                "kb_refresh_failed",
                error=error_msg,
                duration_seconds=round(duration, 2)
            )
            
            # Keep "last known good" by not swapping files
            # Temp files remain and can be inspected for debugging
            
            return {
                "status": "failed",
                "error": error_msg,
                "duration_seconds": round(duration, 2),
                "note": "last known good KB retained"
            }
            
        finally:
            self.is_refreshing = False
    
    async def startup_kb_init(self) -> None:
        """
        Initialize KB on application startup.
        This is a blocking operation that must complete before API serves requests.
        """
        logger.info("kb_startup_init_started")
        
        result = await self.refresh_knowledge_base()
        
        if result['status'] == 'failed':
            # Check if we have a "last known good" from previous run
            compiled_rules_path = KB_DIR / "compiled_rules.json"
            if compiled_rules_path.exists():
                logger.warning(
                    "kb_startup_init_failed_using_last_known_good",
                    error=result.get('error')
                )
                # Load existing KB version
                compiled_rules = await rules_compiler.load_compiled_rules()
                if compiled_rules:
                    self.current_version = compiled_rules.get('version')
                    self.last_refresh_status = "using_last_known_good"
            else:
                # No fallback available - this is critical
                logger.error(
                    "kb_startup_init_failed_no_fallback",
                    error=result.get('error')
                )
                raise RuntimeError(
                    f"KB initialization failed and no fallback available: {result.get('error')}"
                )
        else:
            logger.info("kb_startup_init_success", version=result['version'])
    
    def start_scheduler(self) -> None:
        """
        Start the hourly KB refresh scheduler.
        Runs in background after startup initialization.
        """
        self.scheduler.add_job(
            self.refresh_knowledge_base,
            trigger=IntervalTrigger(hours=settings.kb_refresh_interval_hours),
            id='kb_refresh',
            name='KB Hourly Refresh',
            replace_existing=True
        )
        
        self.scheduler.start()
        
        logger.info(
            "kb_scheduler_started",
            interval_hours=settings.kb_refresh_interval_hours
        )
    
    def stop_scheduler(self) -> None:
        """Stop the scheduler gracefully."""
        if self.scheduler.running:
            self.scheduler.shutdown(wait=True)
            logger.info("kb_scheduler_stopped")
    
    def get_status(self) -> dict:
        """Get current KB refresh status for health endpoint."""
        next_refresh = None
        if self.last_refresh_time:
            next_refresh = self.last_refresh_time + timedelta(
                hours=settings.kb_refresh_interval_hours
            )
        
        return {
            "last_refresh": self.last_refresh_time.isoformat() if self.last_refresh_time else None,
            "next_refresh": next_refresh.isoformat() if next_refresh else None,
            "status": self.last_refresh_status,
            "version": self.current_version,
            "error": self.last_refresh_error,
            "is_refreshing": self.is_refreshing
        }


# Global scheduler instance
kb_scheduler = KBRefreshScheduler()
