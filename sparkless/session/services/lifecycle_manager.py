"""
Service for managing session lifecycle.

This module provides the SessionLifecycleManager class, which handles
session startup, shutdown, and resource cleanup operations.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class SessionLifecycleManager:
    """
    Manages session lifecycle operations.

    Handles session startup, shutdown, and resource cleanup to ensure
    proper resource management and prevent memory leaks.
    """

    def stop_session(self, storage: Any, performance_tracker: Any) -> None:
        """Stop session and clean up resources.

        Args:
            storage: Storage backend to clean up.
            performance_tracker: Performance tracker to clear.
        """
        # Close storage connections to prevent leaks
        try:
            if hasattr(storage, "close"):
                storage.close()
        except Exception as e:
            logger.warning(
                "Error closing storage backend during session shutdown: %s", e
            )

        # Clear performance tracking data
        try:
            if hasattr(performance_tracker, "clear_cache"):
                performance_tracker.clear_cache()
        except Exception as e:
            logger.warning(
                "Error clearing performance tracker cache during session shutdown: %s",
                e,
            )

    def cleanup_resources(self, storage: Any) -> None:
        """Clean up storage and other resources.

        Args:
            storage: Storage backend to clean up.
        """
        # Close storage connections
        try:
            if hasattr(storage, "close"):
                storage.close()
        except Exception as e:
            logger.warning(
                "Error closing storage backend during resource cleanup: %s", e
            )
