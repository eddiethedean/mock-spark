"""
Unit tests for lifecycle manager error handling improvements.

Tests that lifecycle manager properly logs errors during cleanup
instead of silently ignoring them.
"""

import logging
import pytest
from unittest.mock import Mock, patch
from sparkless.session.services.lifecycle_manager import SessionLifecycleManager


@pytest.mark.unit
class TestLifecycleManagerErrorHandling:
    """Test lifecycle manager error handling and logging."""

    def test_stop_session_logs_storage_close_error(self):
        """Test that storage close errors are logged."""
        manager = SessionLifecycleManager()
        
        # Create mock storage that raises error on close
        mock_storage = Mock()
        mock_storage.close = Mock(side_effect=Exception("Storage close failed"))
        mock_performance_tracker = Mock()
        
        # Capture log output
        with patch("sparkless.session.services.lifecycle_manager.logger") as mock_logger:
            manager.stop_session(mock_storage, mock_performance_tracker)
            
            # Should log warning
            mock_logger.warning.assert_called()
            call_args = mock_logger.warning.call_args[0][0]
            assert "Error closing storage backend" in call_args

    def test_stop_session_logs_performance_tracker_error(self):
        """Test that performance tracker errors are logged."""
        manager = SessionLifecycleManager()
        
        mock_storage = Mock()
        mock_performance_tracker = Mock()
        mock_performance_tracker.clear_cache = Mock(side_effect=Exception("Cache clear failed"))
        
        # Capture log output
        with patch("sparkless.session.services.lifecycle_manager.logger") as mock_logger:
            manager.stop_session(mock_storage, mock_performance_tracker)
            
            # Should log warning
            mock_logger.warning.assert_called()
            call_args = mock_logger.warning.call_args[0][0]
            assert "Error clearing performance tracker cache" in call_args

    def test_stop_session_handles_successful_cleanup(self):
        """Test that successful cleanup doesn't log errors."""
        manager = SessionLifecycleManager()
        
        mock_storage = Mock()
        mock_performance_tracker = Mock()
        
        # Capture log output
        with patch("sparkless.session.services.lifecycle_manager.logger") as mock_logger:
            manager.stop_session(mock_storage, mock_performance_tracker)
            
            # Should not log warnings if cleanup succeeds
            mock_logger.warning.assert_not_called()

    def test_cleanup_resources_logs_storage_error(self):
        """Test that resource cleanup errors are logged."""
        manager = SessionLifecycleManager()
        
        mock_storage = Mock()
        mock_storage.close = Mock(side_effect=Exception("Cleanup failed"))
        
        # Capture log output
        with patch("sparkless.session.services.lifecycle_manager.logger") as mock_logger:
            manager.cleanup_resources(mock_storage)
            
            # Should log warning
            mock_logger.warning.assert_called()
            call_args = mock_logger.warning.call_args[0][0]
            assert "Error closing storage backend during resource cleanup" in call_args

    def test_cleanup_resources_handles_missing_close_method(self):
        """Test that missing close method is handled gracefully."""
        manager = SessionLifecycleManager()
        
        # Storage without close method
        mock_storage = Mock(spec=[])  # No close method
        
        # Should not raise exception
        with patch("sparkless.session.services.lifecycle_manager.logger") as mock_logger:
            manager.cleanup_resources(mock_storage)
            
            # Should not log if hasattr returns False
            # (close method doesn't exist, so it's skipped)
            mock_logger.warning.assert_not_called()

    def test_stop_session_continues_after_storage_error(self):
        """Test that stop_session continues cleanup after storage error."""
        manager = SessionLifecycleManager()
        
        mock_storage = Mock()
        mock_storage.close = Mock(side_effect=Exception("Storage error"))
        mock_performance_tracker = Mock()
        
        # Should continue to performance tracker cleanup
        with patch("sparkless.session.services.lifecycle_manager.logger"):
            manager.stop_session(mock_storage, mock_performance_tracker)
            
            # Performance tracker cleanup should still be attempted
            if hasattr(mock_performance_tracker, "clear_cache"):
                mock_performance_tracker.clear_cache.assert_called()

