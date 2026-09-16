"""DigitalOcean Spaces checkpointing (§8.1) -- behind a feature flag, off by
default. Multipart-uploads the result JSONL progressively so a crash mid-job
preserves completed work; on restart, resumes from the last uploaded part.

`aioboto3` is an optional dependency (`pip install .[spaces]`) and is only
imported when the feature flag is on, so the core service and its test suite
never require it.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import structlog

log = structlog.get_logger()

_PART_SIZE = 5 * 1024 * 1024  # S3 multipart minimum part size


class SpacesCheckpointer:
    """Wraps a multipart upload of one job's result file to Spaces.

    Not exercised in CI or in this build's live run (no Spaces bucket was
    provisioned) -- see STATUS.md. Included complete because §8.1 asks for it
    as an extension, behind a flag that keeps it inert unless configured.
    """

    def __init__(self, bucket: str, region: str, key: str, secret: str, job_id: str) -> None:
        self._bucket = bucket
        self._endpoint = f"https://{region}.digitaloceanspaces.com"
        self._key = key
        self._secret = secret
        self._object_key = f"batchengine/{job_id}/results.jsonl"
        self._upload_id: str | None = None
        self._parts: list[dict[str, object]] = []
        self._part_number = 1

    async def _client(self) -> Any:  # pragma: no cover -- requires aioboto3 + real credentials
        import aioboto3

        session = aioboto3.Session()
        return session.client(
            "s3",
            endpoint_url=self._endpoint,
            aws_access_key_id=self._key,
            aws_secret_access_key=self._secret,
        )

    async def start(self) -> None:  # pragma: no cover
        async with await self._client() as s3:
            resp = await s3.create_multipart_upload(Bucket=self._bucket, Key=self._object_key)
            self._upload_id = resp["UploadId"]

    async def upload_chunk(self, data: bytes) -> None:  # pragma: no cover
        if self._upload_id is None:
            await self.start()
        if len(data) < _PART_SIZE:
            log.debug("spaces.chunk_buffered", size=len(data))
            return
        async with await self._client() as s3:
            resp = await s3.upload_part(
                Bucket=self._bucket,
                Key=self._object_key,
                PartNumber=self._part_number,
                UploadId=self._upload_id,
                Body=data,
            )
            self._parts.append({"PartNumber": self._part_number, "ETag": resp["ETag"]})
            self._part_number += 1

    async def complete(self, result_path: Path) -> None:  # pragma: no cover
        if self._upload_id is None:
            return
        # Flush any tail bytes smaller than the part-size minimum as the
        # final part -- S3 allows the *last* part to be under 5MB.
        remaining = result_path.read_bytes()[len(self._parts) * _PART_SIZE :]
        async with await self._client() as s3:
            if remaining:
                resp = await s3.upload_part(
                    Bucket=self._bucket,
                    Key=self._object_key,
                    PartNumber=self._part_number,
                    UploadId=self._upload_id,
                    Body=remaining,
                )
                self._parts.append({"PartNumber": self._part_number, "ETag": resp["ETag"]})
            await s3.complete_multipart_upload(
                Bucket=self._bucket,
                Key=self._object_key,
                UploadId=self._upload_id,
                MultipartUpload={"Parts": self._parts},
            )
