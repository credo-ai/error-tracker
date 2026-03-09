defmodule ErrorTracker.PrefixTest do
  use ErrorTracker.Test.Case

  describe "set_prefix/1 and get_prefix/0" do
    test "returns nil when no prefix is set" do
      assert ErrorTracker.get_prefix() == nil
    end

    test "round-trips a prefix value" do
      ErrorTracker.set_prefix("error_tracker_tenant_a")
      assert ErrorTracker.get_prefix() == "error_tracker_tenant_a"
    end

    test "returns the prefix that was set" do
      result = ErrorTracker.set_prefix("my_prefix")
      assert result == "my_prefix"
    end

    test "overrides a previously set prefix" do
      ErrorTracker.set_prefix("first")
      ErrorTracker.set_prefix("second")
      assert ErrorTracker.get_prefix() == "second"
    end

    test "is isolated per process" do
      ErrorTracker.set_prefix("parent_prefix")

      task =
        Task.async(fn ->
          # Child process should not see parent's prefix
          assert ErrorTracker.get_prefix() == nil
          ErrorTracker.set_prefix("child_prefix")
          assert ErrorTracker.get_prefix() == "child_prefix"
        end)

      Task.await(task)

      # Parent's prefix should be unaffected
      assert ErrorTracker.get_prefix() == "parent_prefix"
    end
  end
end
