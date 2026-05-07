"""GCS mirror upload + Pub/Sub publish for the realtime pipeline.

This module is invoked at the very end of ``preview-realtime.py`` to:

1. Mirror the daily CSVs that downstream consumers (currently fun-site)
   need to a Cloud Storage bucket. The repository structure is preserved
   under ``gs://${BOATRACE_GCS_CSV_BUCKET}/data/...``.

   Mirrored objects:
   * ``data/programs/title/YYYY/MM/DD.csv`` (daily-sync の成果物)
   * ``data/programs/race_cards/YYYY/MM/DD.csv`` (daily-sync の成果物)
   * ``data/previews/stt/YYYY/MM/DD.csv`` (preview-realtime が追記)
   * ``data/estimate/index/YYYY/MM/DD.csv`` (preview-realtime が更新)

   Each object is uploaded only when its content (md5) differs from the
   currently-stored object. This keeps GCS object generations stable and
   makes the downstream "etag-based early return" cheap.

2. Publish a single ``realtime-completed`` Pub/Sub message describing the
   races whose CSVs changed in this invocation. The message is consumed
   by fun-site's Cloud Run Job via Eventarc.

GCP credentials use Application Default Credentials, which on Cloud Run
Jobs come from the runner service account.

This module is a no-op when ``BOATRACE_GCS_CSV_BUCKET`` is unset, so
unit tests and local invocations of preview-realtime.py keep working
without touching GCP.
"""

from __future__ import annotations

import base64
import csv
import datetime as dt
import hashlib
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

from . import logger as logging_module

# Imports are deferred so that the module can be imported even if the GCP
# client libraries are not yet installed (e.g. during pure-Python unit tests).
try:
    from google.api_core import exceptions as gcp_exceptions
    from google.cloud import pubsub_v1, storage
except ImportError:  # pragma: no cover — exercised at runtime only
    gcp_exceptions = None  # type: ignore[assignment]
    pubsub_v1 = None  # type: ignore[assignment]
    storage = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_BUCKET = os.environ.get("BOATRACE_GCS_CSV_BUCKET", "")
DEFAULT_TOPIC = os.environ.get(
    "BOATRACE_PUBSUB_TOPIC", ""
)  # e.g. "projects/boatrace-487212/topics/realtime-completed"

JST = dt.timezone(dt.timedelta(hours=9))


@dataclass
class CsvUploadSpec:
    """One CSV file to mirror to GCS."""

    csv_type: str  # "title" / "race_cards" / "stt" / "index"
    repo_relative_path: str  # "data/programs/title/2026/05/06.csv" 等


@dataclass
class UploadResult:
    spec: CsvUploadSpec
    changed: bool
    md5: Optional[str] = None


@dataclass
class UpdatedRace:
    race_code: str
    stadium_id: str
    race_number: int
    csv_types: Set[str] = field(default_factory=set)
    index_state: Optional[str] = None  # "daily" or "realtime"

    def to_dict(self) -> dict:
        d = {
            "raceCode": self.race_code,
            "stadiumId": self.stadium_id,
            "raceNumber": self.race_number,
            "csvTypes": sorted(self.csv_types),
        }
        if self.index_state:
            d["indexState"] = self.index_state
        return d


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _file_md5(path: Path) -> str:
    h = hashlib.md5()  # noqa: S324 (GCS uses MD5 for object integrity)
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _gcs_md5_hex(blob) -> Optional[str]:
    """GCS の md5_hash プロパティ (base64) を hex に変換。未設定なら None。"""
    raw = getattr(blob, "md5_hash", None)
    if not raw:
        return None
    try:
        return base64.b64decode(raw).hex()
    except (ValueError, TypeError):
        return None


def _build_csv_specs(repo: Path, day: dt.date) -> List[CsvUploadSpec]:
    """fun-site が当日ビルドで読む 4 種類の CSV のローカル相対パスを列挙。"""
    ymd = f"{day:%Y}/{day:%m}/{day:%d}"
    return [
        CsvUploadSpec("title", f"data/programs/title/{ymd}.csv"),
        CsvUploadSpec("race_cards", f"data/programs/race_cards/{ymd}.csv"),
        CsvUploadSpec("stt", f"data/previews/stt/{ymd}.csv"),
        CsvUploadSpec("index", f"data/estimate/index/{ymd}.csv"),
    ]


# ---------------------------------------------------------------------------
# GCS upload
# ---------------------------------------------------------------------------


def upload_csvs(
    repo: Path,
    day: dt.date,
    bucket_name: Optional[str] = None,
) -> List[UploadResult]:
    """Mirror daily CSVs to GCS, skipping unchanged objects.

    Returns one ``UploadResult`` per spec. ``UploadResult.changed=True``
    means the object was newly uploaded in this call. ``changed=False``
    means either the local file was missing, or its content matched the
    currently-stored object (so no upload happened).
    """
    bucket_name = bucket_name or DEFAULT_BUCKET
    if not bucket_name:
        logging_module.info("gcs_upload_skipped", reason="no_bucket_configured")
        return []
    if storage is None:  # pragma: no cover
        logging_module.error(
            "gcs_upload_skipped",
            reason="google_cloud_storage_not_installed",
        )
        return []

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    results: List[UploadResult] = []
    for spec in _build_csv_specs(repo, day):
        local_path = repo / spec.repo_relative_path
        if not local_path.exists():
            logging_module.info(
                "gcs_upload_skipped",
                csv_type=spec.csv_type,
                reason="local_file_missing",
                path=spec.repo_relative_path,
            )
            results.append(UploadResult(spec=spec, changed=False))
            continue

        local_md5 = _file_md5(local_path)
        blob = bucket.blob(spec.repo_relative_path)
        try:
            blob.reload()
            remote_md5 = _gcs_md5_hex(blob)
            if remote_md5 == local_md5:
                logging_module.info(
                    "gcs_upload_unchanged",
                    csv_type=spec.csv_type,
                    path=spec.repo_relative_path,
                )
                results.append(UploadResult(spec=spec, changed=False, md5=local_md5))
                continue
        except gcp_exceptions.NotFound:
            pass  # blob does not exist yet — first upload

        blob.upload_from_filename(
            str(local_path),
            content_type="text/csv; charset=utf-8",
        )
        logging_module.info(
            "gcs_upload_success",
            csv_type=spec.csv_type,
            path=spec.repo_relative_path,
            md5=local_md5,
        )
        results.append(UploadResult(spec=spec, changed=True, md5=local_md5))

    return results


# ---------------------------------------------------------------------------
# updatedRaces assembly
# ---------------------------------------------------------------------------


def _enumerate_races_in_csv(repo: Path, csv_relative_path: str) -> List[Tuple[str, str, int]]:
    """race_cards CSV から (race_code, stadium_id, race_number) のリストを返す。"""
    path = repo / csv_relative_path
    if not path.exists():
        return []
    races: List[Tuple[str, str, int]] = []
    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            race_code = (row.get("レースコード") or "").strip()
            if not race_code:
                continue
            stadium_id = (row.get("レース場コード") or "").strip()
            race_number_raw = (row.get("レース回") or "").strip()
            race_number = 0
            for ch in race_number_raw:
                if ch.isdigit():
                    race_number = race_number * 10 + int(ch)
            if not stadium_id or race_number == 0:
                continue
            races.append((race_code, stadium_id, race_number))
    return races


def assemble_updated_races(
    repo: Path,
    day: dt.date,
    upload_results: List[UploadResult],
    realtime_updated_codes: Iterable[str],
    *,
    realtime_index_state: str = "realtime",
) -> Tuple[List[UpdatedRace], str]:
    """Build the ``updatedRaces`` payload + an overall ``trigger`` label.

    Logic:
    * If ``programs/title`` or ``programs/race_cards`` is in the changed set,
      treat this run as a *daily bootstrap* and include every race from
      ``programs/race_cards`` in the payload with the corresponding csv types.
    * Otherwise, only include races present in ``realtime_updated_codes``
      (preview-realtime's per-invocation set).
    """
    changed_types: Set[str] = {r.spec.csv_type for r in upload_results if r.changed}

    by_code: Dict[str, UpdatedRace] = {}

    is_bootstrap = bool(changed_types & {"title", "race_cards"})
    trigger = "daily-bootstrap" if is_bootstrap else "realtime"

    if is_bootstrap:
        ymd = f"{day:%Y}/{day:%m}/{day:%d}"
        for race_code, stadium_id, race_number in _enumerate_races_in_csv(
            repo, f"data/programs/race_cards/{ymd}.csv"
        ):
            entry = by_code.setdefault(
                race_code,
                UpdatedRace(
                    race_code=race_code,
                    stadium_id=stadium_id,
                    race_number=race_number,
                ),
            )
            for t in ("title", "race_cards"):
                if t in changed_types:
                    entry.csv_types.add(t)
            if "stt" in changed_types:
                entry.csv_types.add("stt")
            if "index" in changed_types:
                entry.csv_types.add("index")
                entry.index_state = "daily"

    # 直前バッチで実際に更新されたレースは csvTypes を上書き / 追加し、index は realtime 扱い
    realtime_set = {code for code in realtime_updated_codes if code}
    if realtime_set:
        ymd = f"{day:%Y}/{day:%m}/{day:%d}"
        race_card_index: Dict[str, Tuple[str, int]] = {
            code: (sid, num)
            for code, sid, num in _enumerate_races_in_csv(
                repo, f"data/programs/race_cards/{ymd}.csv"
            )
        }
        for code in sorted(realtime_set):
            sid_num = race_card_index.get(code)
            if not sid_num:
                continue  # race_cards に存在しない race_code は無視
            stadium_id, race_number = sid_num
            entry = by_code.setdefault(
                code,
                UpdatedRace(
                    race_code=code,
                    stadium_id=stadium_id,
                    race_number=race_number,
                ),
            )
            if "stt" in changed_types:
                entry.csv_types.add("stt")
            if "index" in changed_types:
                entry.csv_types.add("index")
                entry.index_state = realtime_index_state

    return sorted(by_code.values(), key=lambda r: (r.stadium_id, r.race_number)), trigger


# ---------------------------------------------------------------------------
# Pub/Sub publish
# ---------------------------------------------------------------------------


def publish_realtime_completed(
    day: dt.date,
    updated_races: List[UpdatedRace],
    trigger: str,
    bucket_name: Optional[str] = None,
    topic_path: Optional[str] = None,
    extra_attributes: Optional[Dict[str, str]] = None,
) -> Optional[str]:
    """Publish a single message to the ``realtime-completed`` topic.

    Returns the published message id, or ``None`` if publishing was skipped
    (no topic configured or client library missing).
    """
    bucket_name = bucket_name or DEFAULT_BUCKET
    topic_path = topic_path or DEFAULT_TOPIC
    if not topic_path:
        logging_module.info(
            "pubsub_publish_skipped", reason="no_topic_configured"
        )
        return None
    if pubsub_v1 is None:  # pragma: no cover
        logging_module.error(
            "pubsub_publish_skipped",
            reason="google_cloud_pubsub_not_installed",
        )
        return None

    payload = {
        "publishedAt": dt.datetime.now(JST).isoformat(),
        "raceDate": day.isoformat(),
        "trigger": trigger,
        "updatedRaces": [r.to_dict() for r in updated_races],
        "gcsPrefix": (
            f"gs://{bucket_name}/data/" if bucket_name else None
        ),
    }
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    publisher = pubsub_v1.PublisherClient()
    attrs = {"trigger": trigger, "raceDate": day.isoformat()}
    if extra_attributes:
        attrs.update(extra_attributes)
    future = publisher.publish(topic_path, body, **attrs)
    message_id = future.result(timeout=30)
    logging_module.info(
        "pubsub_publish_success",
        topic=topic_path,
        message_id=message_id,
        trigger=trigger,
        updated_races=len(updated_races),
    )
    return message_id
