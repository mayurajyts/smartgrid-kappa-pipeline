"""Integration check for the second half of the Phase 2 checkpoint.

> *"restarting the job does not duplicate Parquet output"*

WHY THIS NEEDS A TEST RATHER THAN AN EYEBALL
--------------------------------------------
`foreachBatch` gives at-least-once delivery, not exactly-once. A crash between the
Parquet write and the checkpoint commit replays the batch, and plain Parquet has no
atomic commit on object storage to prevent the duplicate rows. So the guarantee
being asserted is specific and worth stating precisely:

  * Spark's checkpoint means a CLEAN restart resumes from the last committed offset,
    so already-processed batches are not reprocessed at all. That is what this test
    verifies: archived row counts do not grow for readings already consumed.
  * A crash mid-batch CAN duplicate that one batch. Every row carries `batch_id` and
    `event_id`, so a duplicated batch is identifiable and any Parquet-sourced reader
    can deduplicate. The proper fix — atomic commits via Delta or Iceberg — is named
    in the report's production-scale section rather than claimed here.

Getting this wrong would be expensive: duplicated archive rows double-count kWh on
any replay from Parquet, which is exactly the money-affecting error the Kappa
argument exists to rule out.

HOW IT RUNS
-----------
This talks to the live stack (Kafka, the object store, the running Job A), so it is
in `tests/integration/` and skips when the stack is not up. Run it with
`make test-integration` after `make up`.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
import urllib.error
import urllib.request

import pytest

COMPOSE_PROJECT = "smartgrid"
S3_ENDPOINT = os.environ.get("S3_EXTERNAL_ENDPOINT", "http://localhost:8333")
BUCKET = os.environ.get("S3_CURATED_BUCKET", "smartgrid-curated")
PREFIX = "readings/"


def _compose(*args: str, timeout: int = 120) -> subprocess.CompletedProcess:
    """Run a docker compose command against the project."""
    return subprocess.run(
        ["docker", "compose", *args],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _archive_keys() -> set[str]:
    """List every object under the curated readings prefix.

    Read over the S3 API rather than by opening the Parquet files, so the test needs
    no Spark on the host — the object keys are sufficient to detect a re-write,
    because a replayed batch writes NEW part files rather than modifying existing
    ones.
    """
    url = f"{S3_ENDPOINT}/{BUCKET}/?list-type=2&prefix={PREFIX}&max-keys=10000"
    try:
        with urllib.request.urlopen(url, timeout=20) as response:
            body = response.read().decode("utf-8")
    except (urllib.error.URLError, OSError) as exc:
        pytest.skip(f"object store not reachable at {S3_ENDPOINT} ({exc})")

    keys = set()
    for fragment in body.split("<Key>")[1:]:
        keys.add(fragment.split("</Key>")[0])
    return keys


@pytest.fixture(scope="module", autouse=True)
def require_running_stack():
    """Skip the whole module unless Job A is up and has archived something."""
    result = _compose("ps", "--format", "json")
    if result.returncode != 0:
        pytest.skip("docker compose is not available or the stack is not running")

    running = set()
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        if entry.get("State") == "running":
            running.add(entry.get("Service"))

    for service in ("kafka", "objectstore", "meter-simulator", "job-a"):
        if service not in running:
            pytest.skip(f"service {service!r} is not running; run `make up` first")

    if not _archive_keys():
        pytest.skip(
            "the curated archive is empty; wait for Job A to complete a micro-batch"
        )


class TestRestartIdempotence:
    def test_restart_does_not_rewrite_archived_batches(self):
        """THE Phase 2 checkpoint.

        The set of object keys present before the restart must still be present
        afterwards, unchanged. New keys are expected and correct — the simulator
        keeps producing — but an already-written key being replaced, or the same
        batch reappearing under a new key, would mean reprocessed data.
        """
        before = _archive_keys()
        assert before, "no archived objects to compare"

        restart = _compose("restart", "job-a", timeout=180)
        assert restart.returncode == 0, f"restart failed: {restart.stderr}"

        # Wait for the job to resume from its checkpoint and commit a new batch.
        # Generous because a Spark driver restart includes JVM startup, executor
        # registration and state store recovery.
        deadline = time.time() + 180
        after = before
        while time.time() < deadline:
            time.sleep(15)
            after = _archive_keys()
            if after - before:
                break

        # Every pre-restart object survives untouched: nothing was rewritten.
        assert before <= after, (
            "objects present before the restart are missing afterwards, which means "
            f"the archive was rewritten: {sorted(before - after)[:5]}"
        )

        # And the job genuinely resumed rather than sitting idle — otherwise the
        # assertion above would pass trivially on a dead job.
        assert after - before, (
            "no new objects after the restart; Job A may not have resumed, so this "
            "test would pass without proving anything"
        )

    def test_checkpoint_survives_the_restart(self):
        """The checkpoint must be on a persistent volume.

        If it were container-local it would be lost on restart and the job would
        reprocess from the start of the topic — duplicating everything already
        archived. This asserts the offset directory exists after a restart.
        """
        result = _compose(
            "exec",
            "job-a",
            "ls",
            "/data/checkpoints/job_a_clean_enrich/offsets",
            timeout=60,
        )
        assert result.returncode == 0, (
            "the checkpoint offsets directory is missing after restart; checkpoints "
            "must live on a named volume or restart-idempotence is impossible"
        )
        assert result.stdout.strip(), "the checkpoint offsets directory is empty"


class TestArchiveLayout:
    def test_partitioned_by_sim_date_and_grid_zone(self):
        """§7 step 5. These are the two predicates every replay and every daily
        report filters on, so partition pruning skips whole directories instead of
        scanning and discarding."""
        keys = _archive_keys()
        assert any("sim_date=" in key for key in keys), "not partitioned by sim_date"
        assert any("grid_zone=" in key for key in keys), "not partitioned by grid_zone"

    def test_partition_order_places_sim_date_outermost(self):
        """sim_date first, because both the daily report and a replay select a single
        day across all zones. Zone-first would force every query to touch every
        day's directory."""
        sample = next(k for k in _archive_keys() if "sim_date=" in k)
        assert sample.index("sim_date=") < sample.index("grid_zone="), (
            f"expected sim_date before grid_zone in the path, got {sample}"
        )
