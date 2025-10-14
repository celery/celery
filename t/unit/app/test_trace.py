"""Unit tests for celery.app.trace module."""

import sys

from celery.app.trace import traceback_clear


class test_traceback_clear:
    """Unit tests for traceback_clear function."""

    def test_uses_exc_argument(self):
        """Test that traceback_clear(exc) correctly uses the exc argument.

        This test proves that the reported issue about traceback_clear not using
        the exc argument is NOT valid. The function does use the exc argument correctly.
        """
        # Create exception with traceback
        def create_exception_with_traceback():
            """Create an exception with a traceback for testing."""
            try:
                # Create a nested call stack to have frames to clear
                def inner_function():
                    x = "some_local_variable" * 1000  # Create local variable  # noqa: F841
                    y = list(range(1000))  # Another local variable  # noqa: F841
                    raise ValueError("Test exception with traceback")

                def outer_function():
                    z = "outer_local_variable" * 1000  # Local variable in outer frame  # noqa: F841
                    inner_function()

                outer_function()
            except Exception as e:
                return e

        # Test 1: traceback_clear(exc) with provided exception
        exc = create_exception_with_traceback()

        # Verify exception has traceback
        exc_tb = getattr(exc, '__traceback__', None)
        assert exc_tb is not None, "Exception should have traceback"

        # Count initial frames
        initial_frames = []
        tb = exc_tb
        while tb is not None:
            initial_frames.append(tb.tb_frame)
            tb = tb.tb_next

        assert len(initial_frames) > 0, "Should have traceback frames"

        # Verify frames have local variables before clearing
        frame_locals_before = []
        for frame in initial_frames:
            frame_locals_before.append(len(frame.f_locals))

        assert any(count > 0 for count in frame_locals_before), "Frames should have local variables"

        # Call traceback_clear with the exception - this should use exc argument
        traceback_clear(exc)

        # Verify frames are cleared
        exc_tb_after = getattr(exc, '__traceback__', None)
        assert exc_tb_after is not None, "Traceback should still exist after clearing"

        tb = exc_tb_after
        frames_after = []
        while tb is not None:
            frames_after.append(tb.tb_frame)
            tb = tb.tb_next

        # Check that frame locals are cleared
        cleared_count = 0
        for frame in frames_after:
            if len(frame.f_locals) == 0:
                cleared_count += 1

        assert cleared_count == len(frames_after), "All frames should be cleared"

        # Verify the function actually used the exc argument by checking traceback still exists
        assert getattr(exc, '__traceback__', None) is not None, (
            "Traceback should still exist but frames should be cleared"
        )

    def test_without_exc_argument(self):
        """Test traceback_clear() without exc argument uses sys.exc_info()."""
        try:
            def test_function():
                local_var = "test" * 1000  # noqa: F841
                raise RuntimeError("Test exception")

            test_function()
        except Exception:
            # Now we're in except block with active traceback
            _, _, tb_before = sys.exc_info()
            assert tb_before is not None, "Should have active traceback"

            # Call traceback_clear without argument - should use sys.exc_info()
            traceback_clear()
            # Test passes if no exception is raised

    def test_with_none(self):
        """Test traceback_clear(None) uses sys.exc_info() fallback."""
        try:
            def test_function():
                local_var = "test" * 1000  # noqa: F841
                raise RuntimeError("Test exception")

            test_function()
        except Exception:
            # Call with None - should fall back to sys.exc_info()
            traceback_clear(None)
            # Test passes if no exception is raised

    def test_with_exception_no_traceback(self):
        """Test traceback_clear with exception that has no __traceback__."""
        # Create exception without traceback
        exc = ValueError("Test exception")

        # Should not raise exception
        traceback_clear(exc)

    def test_handles_runtime_error(self):
        """Test that traceback_clear handles RuntimeError when frame is executing."""
        # This test is mainly for coverage - RuntimeError handling is internal
        # and difficult to trigger in normal circumstances
        try:
            def test_function():
                local_var = "test" * 1000  # noqa: F841
                raise RuntimeError("Test exception")

            test_function()
        except Exception as exc:
            # Should not raise exception even if RuntimeError occurs internally
            traceback_clear(exc)
