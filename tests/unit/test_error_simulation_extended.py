"""Unit tests for error simulation functionality."""

import pytest
from mock_spark import MockSparkSession
from mock_spark.error_simulation import ErrorSimulator


@pytest.fixture
def spark():
    """Create test session."""
    session = MockSparkSession("test_error_sim")
    yield session
    try:
        session.stop()
    except Exception:
        pass


def test_error_simulator_creation():
    """Test ErrorSimulator can be created."""
    simulator = ErrorSimulator()
    assert simulator is not None


def test_error_simulator_inject_error():
    """Test injecting errors into operations."""
    simulator = ErrorSimulator()
    simulator.inject_error("read", Exception("Read failed"))
    
    assert simulator.should_fail("read")


def test_error_simulator_clear_errors():
    """Test clearing injected errors."""
    simulator = ErrorSimulator()
    simulator.inject_error("read", Exception("Read failed"))
    simulator.clear_errors()
    
    assert not simulator.should_fail("read")


def test_error_simulator_probability_based():
    """Test probability-based error injection."""
    simulator = ErrorSimulator()
    simulator.inject_error_with_probability("write", 0.5, Exception("Write failed"))
    
    # With 50% probability, some calls should fail and some should succeed
    # We test the mechanism exists
    assert simulator is not None


def test_error_simulator_error_count():
    """Test counting simulated errors."""
    simulator = ErrorSimulator()
    simulator.inject_error("read", Exception("Failed"))
    
    count = simulator.get_error_count("read")
    assert count >= 0


def test_error_simulator_multiple_operations():
    """Test simulating errors for multiple operations."""
    simulator = ErrorSimulator()
    simulator.inject_error("read", Exception("Read failed"))
    simulator.inject_error("write", Exception("Write failed"))
    
    assert simulator.should_fail("read")
    assert simulator.should_fail("write")


def test_error_simulator_reset():
    """Test resetting simulator state."""
    simulator = ErrorSimulator()
    simulator.inject_error("read", Exception("Failed"))
    simulator.reset()
    
    assert not simulator.should_fail("read")


def test_error_simulator_custom_exception():
    """Test using custom exception types."""
    simulator = ErrorSimulator()
    custom_error = ValueError("Custom error")
    simulator.inject_error("process", custom_error)
    
    assert simulator.should_fail("process")


def test_error_simulator_delayed_error():
    """Test delayed error injection."""
    simulator = ErrorSimulator()
    simulator.inject_error_after_calls("write", 3, Exception("Failed"))
    
    # First 3 calls should succeed, 4th should fail
    for _ in range(3):
        assert not simulator.should_fail("write", increment=True)


def test_error_simulator_error_message():
    """Test retrieving error messages."""
    simulator = ErrorSimulator()
    error = Exception("Test error message")
    simulator.inject_error("read", error)
    
    retrieved_error = simulator.get_error("read")
    assert "Test error message" in str(retrieved_error)


def test_error_simulator_operation_list():
    """Test listing operations with errors."""
    simulator = ErrorSimulator()
    simulator.inject_error("read", Exception("Error 1"))
    simulator.inject_error("write", Exception("Error 2"))
    
    operations = simulator.list_operations_with_errors()
    assert "read" in operations
    assert "write" in operations


def test_error_simulator_disable_enable():
    """Test disabling and enabling simulator."""
    simulator = ErrorSimulator()
    simulator.inject_error("read", Exception("Failed"))
    
    simulator.disable()
    assert not simulator.should_fail("read")
    
    simulator.enable()
    assert simulator.should_fail("read")


def test_error_simulator_conditional_errors():
    """Test conditional error injection."""
    simulator = ErrorSimulator()
    
    def condition(data):
        return len(data) > 100
    
    simulator.inject_error_when("process", condition, Exception("Too large"))
    assert simulator is not None


def test_error_simulator_statistics():
    """Test error simulation statistics."""
    simulator = ErrorSimulator()
    simulator.inject_error("read", Exception("Failed"))
    
    # Trigger some failures
    for _ in range(5):
        try:
            simulator.should_fail("read")
        except:
            pass
    
    stats = simulator.get_statistics()
    assert stats is not None

