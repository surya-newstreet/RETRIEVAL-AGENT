"""
Manual KB refresh script.
Use this for troubleshooting or manual KB regeneration.
"""
import asyncio
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from db.connection import db_manager
from scheduler.kb_refresh import kb_scheduler
from observability.logger import get_logger

logger = get_logger(__name__)


async def main():
    """Run manual KB refresh."""
    print("Starting manual KB refresh...")
    print("=" * 60)
    
    try:
        # Initialize database pools
        print("\n1. Initializing database connections...")
        await db_manager.init_metadata_pool()
        print("✓ Metadata pool initialized")
        
        # Run KB refresh
        print("\n2. Running KB refresh...")
        result = await kb_scheduler.refresh_knowledge_base()
        
        print("\n" + "=" * 60)
        print("KB REFRESH RESULT:")
        print("=" * 60)
        
        if result['status'] == 'success':
            print(f"✓ Status: SUCCESS")
            print(f"✓ Version: {result['version']}")
            print(f"✓ Table Count: {result['table_count']}")
            print(f"✓ Duration: {result['duration_seconds']:.2f}s")
            print(f"✓ Timestamp: {result['timestamp']}")
        else:
            print(f"✗ Status: FAILED")
            print(f"✗ Error: {result.get('error', 'Unknown')}")
            print(f"✗ Duration: {result['duration_seconds']:.2f}s")
            if result.get('note'):
                print(f"ℹ Note: {result['note']}")
        
        print("=" * 60)
        
        # Close pools
        await db_manager.close_pools()
        
        return 0 if result['status'] == 'success' else 1
        
    except Exception as e:
        print(f"\n✗ FATAL ERROR: {str(e)}")
        print("=" * 60)
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
