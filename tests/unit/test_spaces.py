"""SpacesCheckpointer's network methods require aioboto3 and real credentials
and are marked `# pragma: no cover` in extensions/spaces.py -- not exercised
in this build (see STATUS.md/OPEN_QUESTIONS.md). This only covers the
inert construction path.
"""

from __future__ import annotations

from batchengine.extensions.spaces import SpacesCheckpointer


def test_checkpointer_constructs_object_key() -> None:
    checkpointer = SpacesCheckpointer(
        bucket="my-bucket", region="nyc3", key="k", secret="s", job_id="job-123"
    )
    assert checkpointer._object_key == "batchengine/job-123/results.jsonl"
    assert checkpointer._endpoint == "https://nyc3.digitaloceanspaces.com"
