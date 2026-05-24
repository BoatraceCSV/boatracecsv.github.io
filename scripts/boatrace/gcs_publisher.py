"""GCS mirror upload + Pub/Sub publish for the realtime pipeline.

This module is invoked at the very end of ``preview-realtime.py`` to:

1. Mirror the daily CSVs that downstream consumers (currently fun-site)
   need to a Cloud Storage bucket. The repository structure is preserved
   under ``gs://${BOATRACE_GCS_CSV_BUCKET}/data/...``.

   Mirrored objects:
   * ``data/programs/title/YYYY/MM/DD.csv`` (daily-sync の成果物)
   * ``data/programs/race_cards/YYYY/MM/DD.csv`` (daily-sync の成果物)
   * ``data/previews/stt/YYYY/MM/DD.csv`` (preview-realtime が追記)
   * ``data/estimate/{predictor_id}/YYYY/MM/DD.csv`` (各 active 予想者 1 件ずつ。
     csv_type は ``index:{predictor_id}`` 形式)
   * ``data/results/realtime/YYYY/MM/DD.csv`` (preview-realtime が追記)
   * ``data/results/payouts/YYYY/MM/DD.csv`` (preview-realtime が追記)

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
from .predictors import active_predictors

# 予想者別 index CSV の csv_type プリフィックス。
# csv_type = f"{INDEX_CSV_TYPE_PREFIX}{predictor_id}"
# 例: "index:v1_basic" / "index:v2_tenkai"
INDEX_CSV_TYPE_PREFIX = "index:"

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
    """One CSV file to mirror to GCS.

    ``csv_type`` is a stable identifier used by fun-site (via Pub/Sub) to
    decide which CSV to re-fetch. Predictor-specific index CSVs encode the
    predictor id as ``"index:<predictor_id>"`` (e.g. ``"index:v1_basic"``);
    fun-site parses this to map back to its own predictor registry.
    """

    csv_type: str  # "title" / "race_cards" / "stt" / "index:<pred>" / "results" / "payouts"
    repo_relative_path: str  # "data/programs/title/2026/05/06.csv" 等


def is_index_csv_type(csv_type: str) -> bool:
    """``csv_type`` が予想者別 index CSV (``index:...``) か判定。"""
    return csv_type.startswith(INDEX_CSV_TYPE_PREFIX)


def predictor_id_from_index_csv_type(csv_type: str) -> str:
    """``"index:v1_basic"`` → ``"v1_basic"``。``index:`` プリフィックスを剥がす。"""
    if not is_index_csv_type(csv_type):
        raise ValueError(f"Not an index csv_type: {csv_type!r}")
    return csv_type[len(INDEX_CSV_TYPE_PREFIX):]


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
    """fun-site が当日ビルドで読む CSV のローカル相対パスを列挙。

    ``results`` は preview-realtime が当日確定直後に追記する realtime 結果
    CSV (``data/results/realtime/YYYY/MM/DD.csv``)。``payouts`` は同じく
    確定直後に bc_rs2 から追記する払戻 CSV
    (``data/results/payouts/YYYY/MM/DD.csv``)。

    Index CSV はレジストリの active 予想者ぶん列挙され、それぞれ
    csv_type=``index:{predictor_id}`` でアップロードされる。
    """
    ymd = f"{day:%Y}/{day:%m}/{day:%d}"
    specs: List[CsvUploadSpec] = [
        CsvUploadSpec("title", f"data/programs/title/{ymd}.csv"),
        CsvUploadSpec("race_cards", f"data/programs/race_cards/{ymd}.csv"),
        CsvUploadSpec("stt", f"data/previews/stt/{ymd}.csv"),
    ]
    for predictor in active_predictors():
        specs.append(CsvUploadSpec(
            csv_type=f"{INDEX_CSV_TYPE_PREFIX}{predictor.predictor_id}",
            repo_relative_path=f"data/estimate/{predictor.predictor_id}/{ymd}.csv",
        ))
    specs.extend([
        CsvUploadSpec("results", f"data/results/realtime/{ymd}.csv"),
        CsvUploadSpec("payouts", f"data/results/payouts/{ymd}.csv"),
    ])
    return specs


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
    result_updated_codes: Optional[Iterable[str]] = None,
    payout_updated_codes: Optional[Iterable[str]] = None,
    *,
    realtime_index_state: str = "realtime",
) -> Tuple[List[UpdatedRace], str]:
    """Build the ``updatedRaces`` payload + an overall ``trigger`` label.

    Logic:
    * If ``programs/title`` or ``programs/race_cards`` is in the changed set,
      treat this run as a *daily bootstrap* and include every race from
      ``programs/race_cards`` in the payload with the corresponding csv types.
    * Otherwise, include races present in ``realtime_updated_codes``
      (preview-realtime が今サイクルで preview 行を追記したレース) and/or
      ``result_updated_codes`` (今サイクルで realtime 結果行を追記した
      レース) and/or ``payout_updated_codes`` (今サイクルで払戻行を追記
      したレース)。「結果のみ」「払戻のみ」が更新されたサイクルでも
      payload が空にならないように、各軸を独立に扱う。
    """
    changed_types: Set[str] = {r.spec.csv_type for r in upload_results if r.changed}
    changed_index_types: Set[str] = {
        t for t in changed_types if is_index_csv_type(t)
    }

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
            if changed_index_types:
                # 予想者別 index CSV をそれぞれ csv_types に追加。
                entry.csv_types.update(changed_index_types)
                entry.index_state = "daily"
            if "results" in changed_types:
                entry.csv_types.add("results")
            if "payouts" in changed_types:
                entry.csv_types.add("payouts")

    # 直前バッチで実際に更新されたレースは csvTypes を上書き / 追加し、index は realtime 扱い
    realtime_set = {code for code in realtime_updated_codes if code}
    result_set = {code for code in (result_updated_codes or ()) if code}
    payout_set = {code for code in (payout_updated_codes or ()) if code}
    if realtime_set or result_set or payout_set:
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
            if changed_index_types:
                entry.csv_types.update(changed_index_types)
                entry.index_state = realtime_index_state

        for code in sorted(result_set):
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
            if "results" in changed_types:
                entry.csv_types.add("results")

        for code in sorted(payout_set):
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
            if "payouts" in changed_types:
                entry.csv_types.add("payouts")

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
