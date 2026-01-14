"""
FastAPI application main entry point.
"""
import os  # NEW: runtime/reload diagnostics
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from api.routes import router
from db.connection import db_manager
from scheduler.kb_refresh import kb_scheduler
from core.config import settings
from observability.logger import get_logger

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager for startup and shutdown."""
    # Startup
    logger.info("application_startup", version="1.0.0")

    # NEW: log process + singleton instance IDs to catch silent reloads / multi-worker issues
    try:
        # Best-effort detection (CLI --reload / watchdog reloaders / etc.)
        uvicorn_env = {k: os.environ.get(k) for k in os.environ.keys() if k.startswith("UVICORN")}
        reload_signals = {
            "UVICORN_RELOAD": os.environ.get("UVICORN_RELOAD"),
            "RUN_MAIN": os.environ.get("RUN_MAIN"),
            "WERKZEUG_RUN_MAIN": os.environ.get("WERKZEUG_RUN_MAIN"),
            "WATCHFILES_FORCE_POLLING": os.environ.get("WATCHFILES_FORCE_POLLING"),
        }

        # Import singletons here (safe) to log stable ids across requests
        from core.context_resolver import context_resolver as _context_resolver  # NEW
        from core.llm_sql_generator import llm_sql_generator as _llm_sql_generator  # NEW

        logger.info(
            "runtime_diagnostics",
            pid=os.getpid(),
            ppid=os.getppid(),
            reload_signals=reload_signals,
            uvicorn_env=uvicorn_env,
            context_resolver_instance_id=id(_context_resolver),
            llm_sql_generator_instance_id=id(_llm_sql_generator),
        )

        # If any hint of reload is present, warn loudly (context is in-memory)
        if any(v for v in reload_signals.values()):
            logger.warning(
                "reload_or_reloader_detected",
                note="In-memory context will reset on process reload. Run without --reload and avoid multi-worker if using in-memory sessions.",
                reload_signals=reload_signals,
                pid=os.getpid(),
            )
    except Exception as e:
        # Never block startup for diagnostics
        logger.warning("runtime_diagnostics_failed", error=str(e))

    try:
        # Initialize database pools
        await db_manager.init_metadata_pool()
        await db_manager.init_query_pool()

        # Initialize knowledge base
        await kb_scheduler.startup_kb_init()

        # Start KB refresh scheduler
        kb_scheduler.start_scheduler()

        logger.info("application_ready")

    except Exception as e:
        logger.error("application_startup_failed", error=str(e))
        raise

    yield

    # Shutdown
    logger.info("application_shutdown")

    kb_scheduler.stop_scheduler()
    await db_manager.close_pools()

    logger.info("application_stopped")


# Create FastAPI app
app = FastAPI(
    title="NL to SQL System",
    description="Production-grade Natural Language to SQL retrieval system",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.api_cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(router, prefix="/api/v1")


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "name": "NL to SQL System",
        "version": "1.0.0",
        "status": "running",
        "docs": "/docs"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "api.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
        workers=1,  # NEW: make single-worker explicit when running via python api/main.py
        log_level=settings.log_level.lower()
    )
