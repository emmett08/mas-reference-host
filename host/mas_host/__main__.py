"""Run the mixed-agent demo or administer its private deployment."""
from __future__ import annotations
import sys


def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments and arguments[0] == 'events':
        from .event_cli import main as event_main
        return event_main(arguments[1:])
    if arguments and arguments[0] == "workers":
        from .distributed_cli import main as workers_main
        return workers_main(arguments[1:])
    if arguments and arguments[0] == "workflow":
        from .workflow_cli import main as workflow_main
        return workflow_main(arguments[1:])
    if arguments and arguments[0] == "bridge":
        from .bridge_cli import main as bridge_main
        return bridge_main(arguments[1:])
    if arguments and arguments[0] == "admin":
        from .admin import main as admin_main
        return admin_main(arguments[1:])
    if arguments and arguments[0] == "jobs":
        from .jobs_cli import main as jobs_main
        return jobs_main(arguments[1:])
    from .demo import main as demo_main
    return demo_main(arguments)


if __name__ == "__main__":
    raise SystemExit(main())
