# FeatureContext リファクタ設計書

`scripts/build_weights.py`(monthly-weights Cloud Run Job)が 3600 秒の
`--task-timeout` を超過してタイムアウトした件の根治策。
`scripts/boatrace/index_features.py::compute_features_for_day` を**バッチ呼出し**
向けに最適化するため、共有キャッシュ `FeatureContext` を新設する。

- 対象スクリプト: `scripts/boatrace/index_features.py`, `scripts/build_weights.py`
- 影響範囲: monthly-weights Job の所要時間(75 分 → 5〜10 分見込み)
- 出力 CSV のスキーマ・値は **byte-identical** を維持(回帰なし前提)

---

## 1. 問題の現状

### 1.1 観測

2026-05-23 の `run-monthly-weights` 実行ログ:

| 進捗 | 経過時間 | 1 日あたり |
| --- | --- | --- |
| 0–30 日 | 5:16 | 10.5 秒 |
| 30–60 日 | +8:53 | 17.8 秒 |
| 60–90 日 | +11:43 | 23.4 秒 |
| 90–120 日 | +12:33 | 25.1 秒 |
| 120–150 日 | +13:03 | 26.1 秒 |
| 150–181 日 | 強制終了 | – |

`build_training_table` の単純外挿で **約 75 分**。3600 秒の task-timeout に収まらない。

### 1.2 根本原因

`compute_features_for_day(repo, day)` が毎日以下を再実行している:

| 処理 | 1 日あたり I/O | 181 日換算 |
| --- | --- | --- |
| `load_waku_table` / `load_motor_score_table` / `load_sui_params` | 3 CSV (静的・不変) | 543 回 |
| `load_motor_period_starts` | 最大 15 日ぶんの `motor_stats/*.csv` 探索 | 〜2,700 回 |
| `detect_session_end_days`(24 場) | 24 場 × 90 日の `race_cards/*.csv` 存在チェック+読込 | **390,960 回** |
| `extract_runs_for_session` | 24 場 × 最大 5 節の `race_cards` + `title` 再読込 | 〜21,720 回 |
| 1 日固有データ(`recent_*`, `previews/*`, 当日 `race_cards`) | 7 CSV | 1,267 回 |

連続する 2 日で参照する `race_cards` 群はほぼ完全に重複しているため、
ファイル I/O の **約 99% が冗長**。これを除去するのが本質的な打ち手。

---

## 2. 設計方針

### 2.1 全体像

`boatrace.index_features` に `FeatureContext` クラスを新設する。

- **単発呼出し用途**(`build_index.py`): 既存どおり `compute_features_for_day(repo, day)`
  を呼ぶ。内部で per-call な FeatureContext がフォールバック構築される。
- **バッチ呼出し用途**(`build_weights.py`): 訓練 window 全体を覆う FeatureContext
  を 1 回だけ構築し、`compute_features_for_day(repo, day, ctx=ctx)` 経由で再利用する。

既存のモジュールレベル関数(`load_motor_history`, `detect_session_end_days`,
`load_waku_table` 等)は**シグネチャを変えずに残す**。
`scripts/tests/unit/test_motor_ability.py` がこれらを直接 import しているため、
破壊的変更を避ける。FeatureContext はその**バッチ最適化された並列実装**。

### 2.2 公開 API

```python
# scripts/boatrace/index_features.py

class FeatureContext:
    """Shared cache for compute_features_for_day across a date window.

    Single-day callers (build_index.py) can ignore this entirely; the
    convenience entry point will construct a per-call context implicitly.
    Multi-day callers (build_weights.py) construct one context up-front
    covering [window_start, window_end], so static tables and file reads
    are amortized.
    """
    def __init__(self, repo: Path, *, window_start: dt.date, window_end: dt.date):
        """NOTE: Not thread-safe. mutable dict キャッシュにロックを持たない。
        将来 multiprocessing を入れる場合は worker ごとに別 Context を持つこと。
        """
        self.window_start = window_start
        self.window_end = window_end
        self._all_stadiums: list[str] = [
            f"{i:02d}" for i in sorted(STADIUM_NAMES.keys())
        ]
        ...

    # ─── 静的テーブル(初期化時に一度だけロード)
    def waku_table(self) -> dict: ...
    def motor_score_table(self) -> dict[tuple[str, str], list[int]]: ...
    def sui_params(self) -> dict: ...

    # ─── キャッシュ付きファイルアクセサ(無制限キャッシュ)
    def race_cards_for(self, day: dt.date) -> pd.DataFrame | None: ...
    def title_for(self, day: dt.date) -> pd.DataFrame | None: ...

    # ─── モーター履歴(セッションインデックスから派生)
    def session_end_days_for(
        self, target_day: dt.date, stadium: str,
    ) -> list[dt.date]: ...
    def motor_history(
        self, target_day: dt.date,
    ) -> dict[tuple[str, int], list[list[MotorRun]]]: ...


def compute_features_for_day(
    repo: Path, day: dt.date, *, ctx: FeatureContext | None = None,
) -> pd.DataFrame:
    """既存の単発 API。ctx 未指定時は per-call FeatureContext を内部構築する。

    ctx を渡す場合、day は必ず ctx の window 内であること。
    window 外の day を渡すと session_index がカバーしておらず motor_history が
    silent に truncate されるため、fail-fast で防御する。
    """
    if ctx is None:
        ctx = FeatureContext(repo, window_start=day, window_end=day)
    elif not (ctx.window_start <= day <= ctx.window_end):
        raise ValueError(
            f"day={day} is outside ctx window "
            f"[{ctx.window_start}, {ctx.window_end}]. "
            f"Construct a Context covering the day, or omit ctx for single-day use."
        )
    ...
```

### 2.3 中核最適化: セッションインデックスの事前計算

最大ボトルネックは `detect_session_end_days(repo, stadium, target_day)` が
181 日 × 24 場 × 90 日遡り = 約 39 万回の `race_cards` 存在チェックを行う点。
window 全体で **1 回だけ**スキャンしてキャッシュに乗せる。

```python
def _build_session_index(self) -> dict[str, list[dt.date]]:
    """全 24 場について、window で参照しうる全 open-day を列挙し
    {stadium: [open_day, ...sorted asc]} を返す。

    リード対象範囲:
      earliest = window_start - MOTOR_HISTORY_LOOKBACK_DAYS (90)
      latest   = window_end  - 1 day   (target_day 当日は除外設計)
    """
    earliest = self.window_start - dt.timedelta(days=MOTOR_HISTORY_LOOKBACK_DAYS)
    latest = self.window_end - dt.timedelta(days=1)
    out: dict[str, list[dt.date]] = {s: [] for s in self._all_stadiums}
    d = earliest
    while d <= latest:
        rc = self.race_cards_for(d)                # race_cards キャッシュ経由
        if rc is not None and not rc.empty:
            codes = rc["レースコード"].dropna().astype(str)
            present = set(codes.str[8:10].unique())
            for s in present:
                if s in out:
                    out[s].append(d)
        d += dt.timedelta(days=1)
    return out
```

各 target_day の `session_end_days_for(day, stadium)` はこの事前インデックスから
派生し、`detect_session_end_days` と **byte-equivalent** な結果を返す。

```python
def session_end_days_for(
    self, target_day: dt.date, stadium: str,
) -> list[dt.date]:
    cutoff_min = target_day - dt.timedelta(days=MOTOR_HISTORY_LOOKBACK_DAYS)
    in_window = [d for d in self._session_index[stadium]
                 if cutoff_min <= d < target_day]
    if not in_window:
        return []
    # 連続日を 1 節として束ねる(既存ロジック踏襲)
    sessions: list[list[dt.date]] = []
    cur = [in_window[0]]
    for d in in_window[1:]:
        if (d - cur[-1]).days <= 1:
            cur.append(d)
        else:
            sessions.append(cur)
            cur = [d]
    sessions.append(cur)
    last_days = [s[-1] for s in sessions]
    return last_days[-MOTOR_HISTORY_LOOKBACK_MAX_SESSIONS:][::-1]
```

これで `race_cards` 存在チェックは **約 39 万回 → 約 270 回**(window+lookback の
総日数)に圧縮される。

### 2.4 race_cards / title のメモリキャッシュ

```python
def race_cards_for(self, day: dt.date) -> pd.DataFrame | None:
    if day not in self._race_cards_cache:
        p = (self.repo / "data" / "programs" / "race_cards"
             / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv")
        self._race_cards_cache[day] = (
            pd.read_csv(p, dtype=str) if p.exists() else None
        )
    return self._race_cards_cache[day]
```

**メモリ見積**: 8 ヶ月 sparse-checkout(≒ 240 日)× 約 30 KB/file ≒ **約 7 MB**。
`title` も同程度。Cloud Run の 2 Gi に対して無視できるサイズなので**無制限**で
保持する(LRU 不要)。

### 2.5 `period_starts` の per-day メモ化

`load_motor_period_starts(repo, day)` は 14 日遡るが、`motor_stats` は小さい
ファイルで呼出し回数も 181 回程度なので、per-day メモ化のみで十分。

```python
def _period_starts(self, day: dt.date) -> dict[tuple[str, int], dt.date]:
    if day not in self._period_starts_cache:
        self._period_starts_cache[day] = load_motor_period_starts(self.repo, day)
    return self._period_starts_cache[day]
```

### 2.6 `motor_history(day)` の再構成

セッション抽出は既存 `extract_runs_for_session` を **memoize するだけ**にする
(関数本体は触らない)。原典の 50 行ほどの非自明な処理 — title CSV 不在時の
`"G2_G3_一般"` フォールバック、`motor_rows` の「先勝ち」辞書、
`D1走1〜D7走2` の 14 スロット展開 — を書き直すと回帰の温床になるため。

```python
def _extract_runs_for_session_cached(
    self, stadium: str, session_end: dt.date,
) -> list[MotorRun]:
    """(stadium, session_end) 単位で extract_runs_for_session を memoize する。

    既存関数をそのまま呼ぶので race_cards / title の二重読みは発生するが、
    呼出し回数の上限は window × 24 場 × 5 節 ≒ 1,350 件で、しかも
    session_end 単位の重複は少ない(同じ session_end が複数の target_day から
    参照されるケース)。memoize で効く規模。
    """
    key = (stadium, session_end)
    if key not in self._runs_cache:
        self._runs_cache[key] = extract_runs_for_session(
            self.repo, stadium, session_end,
        )
    return self._runs_cache[key]

def motor_history(self, day: dt.date) -> dict[tuple[str, int], list[list[MotorRun]]]:
    period_starts = self._period_starts(day)
    out: dict[tuple[str, int], list[list[MotorRun]]] = defaultdict(list)
    for stadium in self._all_stadiums:
        session_ends = self.session_end_days_for(day, stadium)
        per_motor: dict[int, list[list[MotorRun]]] = defaultdict(list)
        for sess_end in session_ends:
            grouped: dict[int, list[MotorRun]] = defaultdict(list)
            for r in self._extract_runs_for_session_cached(stadium, sess_end):
                grouped[r.motor_num].append(r)
            for m, runs in grouped.items():
                per_motor[m].append(runs)
        for m, sessions in per_motor.items():
            ps = period_starts.get((stadium, m))
            if ps is not None:
                sessions = [s for s in sessions if s and s[0].session_end >= ps]
            if sessions:
                out[(stadium, m)] = sessions[:MOTOR_HISTORY_SESSIONS]
    return out
```

### 2.7 `compute_features_for_day` の改修

Context 経由でのアクセスに差し替えるだけ。出力は完全に byte-identical:

```python
def compute_features_for_day(repo, day, *, ctx=None):
    if ctx is None:
        ctx = FeatureContext(repo, window_start=day, window_end=day)

    season = SEASON_BY_MONTH[day.month]
    waku_tab = ctx.waku_table()
    motor_score_table = ctx.motor_score_table()
    motor_history = ctx.motor_history(day)
    sui = ctx.sui_params()

    prog = ctx.race_cards_for(day)               # キャッシュ経由
    if prog is None:
        return pd.DataFrame()

    # recent_national / recent_local / previews 4 種は当日固有なので
    # キャッシュしても効果が薄い。既存どおり都度読みでよい
    # (将来 build_index.py で同日複数回呼ばれる場面が来たら追加検討)
    ...
```

### 2.8 `build_weights.py` の改修

差分は最小限:

```python
def build_training_table(repo: Path, start: dt.date, end: dt.date) -> pd.DataFrame:
    ctx = FeatureContext(repo, window_start=start, window_end=end)
    parts = []
    n_days = (end - start).days + 1
    for i, day in enumerate(iter_dates(start, end)):
        feat = compute_features_for_day(repo, day, ctx=ctx)
        ...
```

---

## 3. テスト戦略

### 3.1 既存テスト

`scripts/tests/unit/test_motor_ability.py` の既存テストはモジュール関数を
直接叩いているため、**シグネチャ変更しないことで全て温存**。

### 3.2 追加テスト

`scripts/tests/unit/test_feature_context.py` を新設:

1. **パリティテスト**: 任意の日 d について
   - `compute_features_for_day(repo, d)` と `compute_features_for_day(repo, d, ctx=ctx)`
     が `DataFrame.equals` で一致
   - `ctx.session_end_days_for(d, s)` と `detect_session_end_days(repo, s, d)` が一致
   - `ctx.motor_history(d)` と `load_motor_history(repo, d)` が一致
   - tmp_path に最小構成のリポジトリを作って window=[d-7, d+7] で複数日検証
2. **境界テスト**: window 端の日(`window_start` ちょうど・`window_end` ちょうど)で
   結果が変わらないこと
3. **空ファイル耐性**: `race_cards` が空 / 不存在の日が window 内にあっても
   セッションインデックス構築が失敗しないこと

### 3.3 統合検証

deploy 前の手動チェック:

- 過去の monthly-weights 出力 CSV(`data/estimate/stadium/index_weights/2026-04.csv`)
  をローカルで再生成し、`git diff` が空であることを確認
- `build_weights.py` は idempotent 設計(`run-monthly-weights.sh` に
  `git diff --cached --quiet` が入っている)ので、本番でも回帰があれば
  即検出される

### 3.4 (任意) CI parity smoke test

tmp_path への合成リポジトリ + `--month` 指定の `build_weights.py` 実行を
CI に追加し、`ctx` あり版と従来版の出力が `DataFrame.equals` で一致する
ことを継続的に検証する。

実リポジトリの sparse checkout を CI で再現するのはコスト高なので、
合成データでの検証で代替可能。本物の月次 CSV との parity は手動チェック
(3.3)に委ねる。

---

## 4. パフォーマンス見積

ファイル I/O 削減効果:

| 項目 | 旧 | 新 |
| --- | --- | --- |
| 静的テーブルロード | 181 × 3 = 543 回 | 3 回 |
| `race_cards` ファイル open | 約 410,000 回 | 約 270 回 |
| `extract_runs_for_session` 呼出し | 約 22,000 回 | 約 1,350 回(memoize 後) |
| `motor_stats` ファイル open | 約 2,700 回 | 約 270 回(per-day memo) |

実時間見積(保守側):

| フェーズ | 旧 | 新 |
| --- | --- | --- |
| `_has_races_at`(usecols=1 read) | 20〜35 分 | 約 3 秒 |
| `extract_runs_for_session`(全列読み + iterrows) | 18〜37 分 | 約 2 分 |
| 当日固有 I/O(`recent_*`, `previews/*`, etc.) | 約 30 秒 | 不変 |
| pandas メインループ(`groupby` / `hensachi` / `iterrows`) | 1〜3 分 | 不変 |
| 静的テーブル + その他オーバーヘッド | 約 10 秒 | 数秒 |
| **build_training_table 合計** | **約 75 分** | **約 10〜15 分** |

**注**: 「5 分」は楽観的下限、「15 分」は保守的上限として扱う。
旧コードの 10.5s/日 → 26s/日 のスループット劣化原因が完全に I/O 起因なら
10 分を切る可能性もあるが、pandas メモリ圧迫や GC pressure など I/O 外の
要因が混じっていればキャッシュ対策では取りきれない(§ 6 リスクを参照)。

---

## 5. ロールアウト計画

### 5.1 PR1: コンテキスト導入(機能完了、`build_weights.py` 未配線)

- `scripts/boatrace/index_features.py`: `FeatureContext` クラス追加、
  `compute_features_for_day` に `ctx=None` キーワード追加
- `scripts/tests/unit/test_feature_context.py`: パリティテスト追加
- (任意) `_build_session_index` / `_extract_runs_for_session_cached` /
  `_period_starts` にデバッグ用のキャッシュ統計を仕込む(§ 5.3)
- `ctx` 未指定時はフォールバックで per-call Context を構築する設計のため、
  **挙動は byte-identical のはず**だが、`build_index.py` 経由の daily-sync も
  新コード経路を必ず通る点に注意。
- **デプロイ後の監視**: PR1 マージ後 1〜2 回の daily-sync 実行ログを能動的に
  確認し、エラー / 出力差分 / 異常な処理時間が出ていないことを目視。

### 5.2 PR2: `build_weights.py` を Context 配線

- `build_training_table` で `FeatureContext` を構築
- `cloudbuild.yaml` の `--task-timeout` は当面 3600s のまま据え置き
  (実測でさらに余裕があれば段階的に短縮)
- マージ後、`gcloud run jobs execute monthly-weights
  --update-env-vars=TARGET_MONTH=2026-04` で過去月を実行し、commit が出ない
  (= 出力に差分が出ない)ことを確認
- **per-30-day スループットの平坦化確認**: ログから 30/60/90/120/150 日各時点の
  スループット(秒/日)が**一定**に近づいているかを目視。旧コードで観測された
  10.5s → 26s への単調劣化が解消されていれば、I/O 起因の劣化が正しく取り除け
  たことの裏付けになる。平坦化していない場合は pandas / GC 由来の二次要因が
  残っているため、追加調査が必要。
- `docs/development.md` に Context の使い方を 1 段落追記

### 5.3 観測性(任意)

`FeatureContext` にキャッシュ統計を持たせ、`build_training_table` 末尾で
ログに出すと PR2 のデプロイ後に「キャッシュが期待どおり効いているか」を
即時検証できる。

```python
# build_weights.py 末尾
print(f"  FeatureContext stats: "
      f"race_cards={len(ctx._race_cards_cache)} "
      f"title={len(ctx._title_cache)} "
      f"runs={len(ctx._runs_cache)} "
      f"period_starts={len(ctx._period_starts_cache)}",
      file=sys.stderr)
```

期待値: 6 ヶ月 window で `race_cards ≒ 270`, `title ≒ 270`,
`runs ≒ 1,350`, `period_starts ≒ 181`。

### 5.4 PR スコープ外(将来検討)

- `build_index.py` を Context に置き換える(perf 改善ゼロのため後回し)
- 並列化(`multiprocessing.Pool`): Context のキャッシュ共有が難しく、また
  上記改修だけで十分速くなるため不要

---

## 6. リスクと対策

| リスク | 対策 |
| --- | --- |
| `session_end_days_for` が `detect_session_end_days` と微妙に非一致 | PR1 でパリティテスト追加。`run-monthly-weights.sh` の `git diff --cached --quiet` で本番でも regression を即検出 |
| race_cards キャッシュのメモリ膨張 | 8 ヶ月 window で約 7 MB と見積もり済。Cloud Run 2 Gi に対して無視可。LRU 不要 |
| `test_motor_ability.py` が壊れる | モジュールレベル関数のシグネチャを一切変えない方針で回避 |
| `motor_history` の period_start フィルタが per-day で正しく動かない | Context が `_period_starts(day)` で per-day に取得・キャッシュする。既存挙動と等価 |
| `build_index.py` の perf 退行 | 単発呼出し時の per-call Context 構築は static loader 3 回 + window=[day,day] のセッションインデックス構築のみ。元コードよりむしろ若干軽い |
| `ctx` の window 外日付呼出しで silent に誤結果 | `compute_features_for_day(repo, day, ctx=ctx)` で `day` が window 外なら `ValueError` で fail-fast(§ 2.2) |
| スループット劣化が pandas/GC 由来で残る | PR2 デプロイ後に per-30-day スループット平坦化を確認(§ 5.2)。劣化が残る場合は build_training_table の pandas メインループ(`hensachi`/`iterrows`)が次の最適化候補 |
| FeatureContext のスレッドセーフ性 | dict キャッシュにロックなし。現状 single-thread で問題なし。docstring に非スレッドセーフを明記、将来 multiprocessing 化時は worker ごとに別 Context を持つ運用とする |

---

## 7. 検討して却下した代替案

| 案 | 概要 | 却下理由 |
| --- | --- | --- |
| `@functools.lru_cache` を直接モジュール関数に適用 | `load_waku_table` 等にデコレータを付けてプロセス全体でキャッシュ | テスト間で cache が leak。tmp_path で別リポジトリを使う `test_motor_ability.py` が壊れる |
| `extract_runs_for_session` を純関数化(`rc_df, tt_df` を引数に取る) | ファイル読みとロジックを分離して再利用 | 綺麗だが PR スコープが膨らみ、回帰サーフェスが広がる。今回は memoize で済ませる |
| session_index を async / thread で並列構築 | 270 ファイル読みを並列化 | 直列でも約 3 秒。複雑度に見合わず却下 |
| 事前計算結果を parquet で永続化 | `compute_features_for_day` 結果を repo にコミット | Cloud Run Job は stateless で次回の Job からは読み出せない。git にコミットすれば再利用可だが diff 肥大の弊害が大きい |
| `multiprocessing.Pool` で日付並列実行 | 181 日を worker で分割 | 各 worker が個別 Context を持つとキャッシュ効果が落ちる。本リファクタ単独で十分な時間短縮が見込まれるため不要 |

## 8. ドキュメント更新範囲

`CLAUDE.md` のドキュメント更新ルールに照らすと、本リファクタは
「CLI 仕様変更なし」「CSV スキーマ変更なし」「workflow/yml 改廃なし」
「派生指標の計算式変更なし」のため**強制更新の対象外**。

任意更新として:

- 本書(`docs/design/feature_context_refactor.md`)を保存(本 PR で実施)
- `docs/development.md` に `FeatureContext` のバッチ用途を 1 段落追記(PR2 で)
