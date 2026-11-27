"""
Formatting helpers for the presentation reports (`cdc.presentation`, `incremental.presentation`): titles, aligned
tables, [OK] checks, and sending the technical output (Spark, notebook logs) to a log file instead of the screen.
"""

import contextlib
import os

WIDTH = 100


def banner(title: str) -> None:
    print("\n" + "=" * WIDTH)
    print(f" {title}")
    print("=" * WIDTH)


def section(title: str) -> None:
    print(f"\n{title}\n" + "-" * len(title))


def line(label: str, text: str) -> None:
    print(f"  {label:<14}{text}")


def check(text: str, ok: bool = True) -> None:
    print(f"  {'[OK]' if ok else '[FAILED]'} {text}")


def n(value) -> str:
    return f"{value:,}"


def shown(value) -> str:
    """A value for a table cell; a missing (null) value as "-"."""
    return "-" if value is None else str(value)


def table(headers: list, rows: list) -> None:
    widths = [max(len(str(h)), *(len(str(r[i])) for r in rows)) for i, h in enumerate(headers)]
    print("  " + " | ".join(str(h).ljust(w) for h, w in zip(headers, widths)))
    print("  " + "-+-".join("-" * w for w in widths))
    for row in rows:
        print("  " + " | ".join(str(v).ljust(w) for v, w in zip(row, widths)))


@contextlib.contextmanager
def quiet(log_file):
    """Send everything the steps print (notebook logs, reader / simulator messages) to the log file."""
    with open(log_file, "a", encoding="utf-8") as log, contextlib.redirect_stdout(log):
        yield


def redirect_stderr_to_log(log_file) -> int:
    """Spark and the JVM write progress bars and warnings to stderr (file descriptor 2); send them to the log.
    Returns the saved descriptor for `restore_stderr`."""
    log_file.parent.mkdir(parents=True, exist_ok=True)
    saved = os.dup(2)
    log_fd = os.open(log_file, os.O_WRONLY | os.O_CREAT | os.O_APPEND)
    os.dup2(log_fd, 2)
    os.close(log_fd)
    return saved


def restore_stderr(saved: int) -> None:
    os.dup2(saved, 2)
    os.close(saved)
