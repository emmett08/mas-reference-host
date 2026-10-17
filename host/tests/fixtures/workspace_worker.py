"""Trusted workspace fixture; all execution is admitted by WorkspaceExecutor."""
import os
from pathlib import Path

from mas_host import Invocation, Outcome
from mas_host.worker_entry import run_worker


class WorkspaceFixture:
    def __init__(self, configuration):
        self.configuration = configuration

    def handle(self, context, request):
        mode = self.configuration.get("mode", "normal")
        if mode == "nested-effect":
            context.invoke(lambda: Invocation({}, {}), reservation_microusd=0)
        source = Path("inputs/source.txt")
        content = source.read_text()
        if mode == "rewrite-input":
            source.chmod(0o600)
            source.write_text("changed")
        output = Path("outputs/result.txt")
        if mode == "symlink":
            output.symlink_to(self.configuration["outside"])
        else:
            output.write_text(content.upper())
        return Outcome({"tool": "uppercase", "workspace": self.configuration["workspace"]["identity"],
                        "cwd": Path.cwd().name, "environment": sorted(os.environ)})


if __name__ == "__main__":
    raise SystemExit(run_worker({"workspace-fixture": WorkspaceFixture}))
