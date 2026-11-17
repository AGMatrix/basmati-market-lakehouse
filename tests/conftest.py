import os
import shutil
import subprocess

import pytest


def _java_home() -> str | None:
    """Spark 4.1 needs Java 17 or 21. On macOS pick one even if a newer default is set."""
    if os.environ.get("JAVA_HOME"):
        return os.environ["JAVA_HOME"]
    if shutil.which("/usr/libexec/java_home"):
        for v in ("17", "21"):
            r = subprocess.run(  # noqa: S603 (fixed binary, fixed args)
                ["/usr/libexec/java_home", "-v", v], capture_output=True, text=True
            )
            if r.returncode == 0:
                return r.stdout.strip()
    return None


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    home = _java_home()
    if home is None and not shutil.which("java"):
        pytest.skip("no Java 17/21 for Spark")
    if home:
        os.environ["JAVA_HOME"] = home
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[2]")
        .appName("tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()
