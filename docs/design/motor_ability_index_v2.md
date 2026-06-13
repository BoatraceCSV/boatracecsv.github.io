# モーター能力指数 v2 設計書(時間減衰 + コース補正 + ベイズ収縮)

v1 ([`motor_ability_index.md`](./motor_ability_index.md)) のスコアテーブル枠組みを
維持しつつ、以下の 3 点を追加する。

1. **時間減衰加重** — 直近の走を相対的に重く、古い走を緩やかに薄める(指数減衰、
   半減期 60 日)。
2. **コース(枠)補正 + SD 標準化** — 級別 × グレード × コースごとの期待点と
   分散をベースラインとし、**z 残差** `(raw − μ_cell) / σ_cell` を集計対象にする。
   1コース 1着と 6コース 1着の価値差・セル間の分散差を同時に吸収。
3. **ベイズ収縮** — サンプル不足のモーター(期切替直後・休場明け等)を事前分布
   (=モーター全体平均=z 残差 0)へ縮める。`k = 10` 相当走の情報量を prior に持たせる。

3 つの仕掛けは **それぞれフィーチャーフラグで個別 ON/OFF 可能**(§2.2 参照)。
全 OFF + `MOTOR_HISTORY_SESSIONS=5` で v1 と算術等価になり、ablation テストと
段階リリースを容易にする。

下流の z スコア化(`build_index.py` の平均 50・SD 10 補正)は据え置きなので、
出力 CSV のスキーマ・スケールは変わらない。**変わるのは値の意味だけ**。

- 対象スクリプト: `scripts/boatrace/index_features.py`
- 対象データ追加: なし(`艇N_節D{D}走{S}_枠` 列が既に race_cards に存在)
- 対象出力列: `data/estimate/index/YYYY/MM/DD.csv` の `N枠_モーターpt`
  (意味のみ再変更、列名据え置き)
- 影響範囲: 重み学習(`build_weights.py`)、月次重み CSV、Strength Index、
  index 監視ダッシュボード

---

## 1. v1 からの差分サマリ

| 観点 | v1(現行) | v2(本設計) |
| --- | --- | --- |
| 集計対象 | 直近 5 節分の全スロット | 直近 **6 節**分の全スロット(減衰でテール薄れる) |
| 各走の重み | 一律 `1` | **`exp(-ln2 × days_ago / 60)`**(`ENABLE_DECAY=False` で `1`) |
| 各走のスコア | 級別 × グレード得点表の値 | **z 残差** `(得点 − μ_cell) / σ_cell`(`ENABLE_LANE_CORRECTION=False` で生得点) |
| 集計式 | 単純平均 = `Σpt / Σruns` | 加重平均(残差)+ ベイズ収縮(`ENABLE_SHRINKAGE=False` で収縮なし) |
| サンプル不足時 | NaN → 下流で 50 補完 | **n_eff に応じて 0(=平均)へ縮める** + 全 0 走で NaN → 50 補完 |
| 必要新カラム | なし | `MotorRun.race_date` / `MotorRun.lane` |
| 必要新ファイル | なし | なし(コース baseline は履歴ウィンドウ内で動的算出) |
| 算術等価性 | — | **全フラグ OFF + `MOTOR_HISTORY_SESSIONS=5` で v1 と一致**(ablation 基底) |

v1 で確定済みの以下要素は **そのまま流用**(再記述しない):

- スコアテーブル CSV(`data/estimate/motor_ability_score.csv`)とその 6 行構造
- グレード判定ルール `grade_bucket_for_grade()` / `resolve_grade_bucket()`
- 失格・棄権の扱い(`MOTOR_NEGATIVE_TOKENS = {転,落,沈,エ} → -100` /
  `MOTOR_SKIP_TOKENS = {F,L,失,妨,欠,不}` → スキップ)
- `normalize_finish_token()`
- 節境界検出 `detect_session_end_days()`(節最終日返却ロジック)
- モーター期起算日テーブル `load_motor_period_starts()` と期境界での剪定
- フェイルセーフ表(v1 §5.4)。本書 §7 で v2 固有の異常系のみ追記。

---

## 2. 確定パラメータ

### 2.1 数値パラメータ

| 項目 | 値 | 根拠 |
| --- | --- | --- |
| (a) 減衰半減期 `H` | **60 日** | 6 節(平均 30 日/節弱)で 1 割前後の重みまで自然減衰。整備による短期変化と長期安定性のバランス点 |
| (b) 減衰定数 `λ` | **`ln(2) / 60 ≈ 0.01155`** | 半減期 60 日からの導出。日数ベース(節カウントベースではない) |
| (c) コース baseline 算出元 | **履歴ウィンドウ内(直近 6 節 × 24 場)** | 24場プール ≈ 12 k 走点。1 セル平均 300+ サンプルで十分な統計力。別ファイル不要 |
| (d) baseline 採用最小サンプル | **5** | (級別,グレード,コース)セルにつき 5 サンプル未満は (級別,グレード) 平均にフォールバック |
| (e) baseline SD 下限 `σ_floor` | **10** | 退化セル(σ → 0)での 0 除算と z 残差暴発防止。スコア表レンジ ~125 の 8% に相当 |
| (f) ベイズ収縮 prior 強度 `k` | **10** | n_eff=10 で 50% 収縮、n_eff=30 で 25% 収縮。新モーター抑制と既存モーター精度の折衷 |
| (g) ベイズ収縮 prior 平均 `μ₀` | **0**(z 残差スケール) | コース補正後の z 残差は構造的に 0 中心なので、prior 平均は 0 で自然 |
| (h) 取得節数 `MOTOR_HISTORY_SESSIONS` | **6**(v1 は 5) | 減衰で 6 節目の寄与は ~13%。これ以上増やしても I/O 増のみで実効寄与は薄い |
| (i) 節検出スキャン上限 `LOOKBACK_MAX_SESSIONS` | **10**(v1 据え置き) | 期境界剪定で削られる分の保険として `MOTOR_HISTORY_SESSIONS × 1.6` を維持 |
| (j) 補完(全走スキップ時) | NaN → 50 | v1 と同じ。下流 z 化で吸収 |

### 2.2 フィーチャーフラグ(リリース段階制御)

3 つの改修ロジックは各々独立 ON/OFF できる。**全 OFF + `MOTOR_HISTORY_SESSIONS=5` で
v1 と算術等価**になることをユニットテストで保証する(§8.1)。

| フラグ | デフォルト | 効果 |
| --- | --- | --- |
| `ENABLE_DECAY` | `True` | False で全走の重み `w_i = 1.0`(等加重) |
| `ENABLE_LANE_CORRECTION` | `True` | False で z 残差化を行わず `residual_i = raw_i`(μ, σ 補正なし) |
| `ENABLE_SHRINKAGE` | `True` | False でベイズ収縮を行わず `motor_pt = mean_resid` |

ablation 検証(§8.3)で各フラグの貢献を切り分けてからリリース判断する。

### 2.3 定数定義

```python
# 時間減衰
ENABLE_DECAY: bool = True
DECAY_HALF_LIFE_DAYS: float = 60.0
DECAY_LAMBDA: float = math.log(2) / DECAY_HALF_LIFE_DAYS   # ≈ 0.01155

# コース補正 / z 残差
ENABLE_LANE_CORRECTION: bool = True
LANE_BASELINE_MIN_SAMPLES: int = 5
LANE_BASELINE_SD_FLOOR: float = 10.0

# ベイズ収縮
ENABLE_SHRINKAGE: bool = True
SHRINKAGE_PRIOR_K: float = 10.0
SHRINKAGE_PRIOR_MEAN: float = 0.0

# 取得節数(v1: 5 → v2: 6)
MOTOR_HISTORY_SESSIONS: int = 6
# 節検出スキャン上限(v1 と同じ)
MOTOR_HISTORY_LOOKBACK_MAX_SESSIONS: int = 10
```

---

## 3. データモデル変更

### 3.1 `MotorRun` への列追加

```python
@dataclass(frozen=True)
class MotorRun:
    session_end: dt.date
    stadium: str
    motor_num: int
    grade_bucket: str
    racer_class: str
    finish: str
    # ↓ v2 追加 ↓
    race_date: dt.date         # この走の実日付(時間減衰の重み計算用)
    lane: int                  # この走でのコース番号 1〜6(コース補正用)
```

`race_date` は時間減衰、`lane` はコース補正に必須なので **両方とも必須フィールド**
(Optional ではない)。取得失敗時はその走を集計から除外する。

### 3.2 ソース列(race_cards)

`艇N_節D{D}走{S}_着順` に加え、以下を読み出す:

| 列 | 用途 | フォールバック順 |
| --- | --- | --- |
| `艇N_節D{D}走{S}_進入` | 実コース(進入後) | 第 1 候補 |
| `艇N_節D{D}走{S}_枠` | 枠番(進入前) | 進入が欠損なら採用 |
| (どちらも欠損) | — | その走をスキップ |

**`進入` を優先する根拠**: 実際にレース展開を決めるのは進入コース。1 枠→1 進入が
9 割なので大半のケースで両者は一致するが、まくり差し・後手などで `枠 ≠ 進入` の
レースがあり、`進入` の方がモーターの真の実力を測る指標として整合的。

### 3.3 `race_date` の確定方法

slot `D{D}走{S}` の日付は `session_start + (D - 1) 日` で求まる。
`session_start` は `detect_session_end_days()` 内部で構築する **連続開催日リスト**
(`cur`)の先頭日付。これを取得するため `detect_session_end_days()` を以下に
リファクタする:

```python
def detect_sessions(
    repo: Path, stadium: str, window_end: dt.date,
    max_sessions: int = MOTOR_HISTORY_SESSIONS,
    window_days: int = MOTOR_HISTORY_LOOKBACK_DAYS,
) -> list[list[dt.date]]:
    """直近 max_sessions 節分の **節日リスト** を新→旧で返す。

    各要素は ``[session_start, ..., session_end]`` の連続開催日。
    既存 ``detect_session_end_days`` は本関数の `[-1]` 抽出ラッパーとして残置可。
    """
    ...
```

v1 互換のため、`detect_session_end_days()` は内部で `detect_sessions()` を呼んで
`[s[-1] for s in sessions]` を返すラッパに置き換える。

slot D が節日数を超える(短い節での D6, D7 等)場合は `finish` が必ず None になり、
そもそも MotorRun が生成されないため `race_date` 算出は不要。

### 3.4 `lane` のパース

`艇N_節D{D}走{S}_進入` / `_枠` の値は文字列または数値。次の関数で正規化する:

```python
def parse_lane(raw_shinnyu, raw_waku) -> int | None:
    """進入優先、欠損なら枠。1〜6 でなければ None。"""
    for raw in (raw_shinnyu, raw_waku):
        if raw is None: continue
        s = str(raw).strip()
        if not s or s.lower() == "nan": continue
        try:
            v = int(float(s))
            if 1 <= v <= 6:
                return v
        except (ValueError, TypeError):
            pass
    return None
```

---

## 4. コース baseline の動的算出(z 残差化)

### 4.1 算出範囲

`load_motor_history()` が組み立てた **全 24 場 × 全節 × 全モーター × 全スロット** の
`MotorRun` を集約して、`(racer_class, grade_bucket, lane)` ごとに **平均と標準偏差**
を計算する。残差は単純な減算ではなく `(raw − μ_cell) / σ_cell` の **z 残差** とする
ことで、セル間の分散異質性を吸収する(レビュー懸念 3 への対応)。

```python
def compute_lane_baseline(
    all_runs: Iterable[MotorRun],
    score_table: dict[tuple[str, str], list[int]],
    min_samples: int = LANE_BASELINE_MIN_SAMPLES,
    sd_floor: float = LANE_BASELINE_SD_FLOOR,
) -> dict[tuple[str, str, int], tuple[float, float]]:
    """Returns: {(racer_class, grade_bucket, lane): (μ, σ)}.

    score_motor_run() で None になる走(F/L/失/妨/欠/不)は集計に含めない。
    転/落/沈/エ は -100 として参加(モーター固有ペナルティと整合)。
    サンプル < min_samples のセルは結果に含めない(呼び出し側でフォールバック)。
    σ は母集団 SD。退化セル(σ < sd_floor)は σ_floor に丸める。
    """
    cells: dict[tuple[str, str, int], list[float]] = defaultdict(list)
    for r in all_runs:
        sc = score_motor_run(score_table, r)
        if sc is None:
            continue
        key = (r.racer_class,
               r.grade_bucket if r.racer_class in ("A1", "A2") else "全",
               r.lane)
        cells[key].append(float(sc[0]))
    out: dict[tuple[str, str, int], tuple[float, float]] = {}
    for key, scores in cells.items():
        if len(scores) < min_samples:
            continue
        mean = sum(scores) / len(scores)
        var = sum((x - mean) ** 2 for x in scores) / len(scores)  # 母集団分散
        sd = max(math.sqrt(var), sd_floor)
        out[key] = (mean, sd)
    return out


def compute_class_grade_avg(
    all_runs: Iterable[MotorRun],
    score_table: dict[tuple[str, str], list[int]],
    min_samples: int = LANE_BASELINE_MIN_SAMPLES,
    sd_floor: float = LANE_BASELINE_SD_FLOOR,
) -> dict[tuple[str, str], tuple[float, float]]:
    """`(racer_class, grade_bucket)` レベルの (μ, σ)。lane baseline の第 1 フォールバック。"""
    # 実装は compute_lane_baseline と同型(key から lane を落とす)
    ...
```

### 4.2 算出粒度の根拠

24 場 × 6 節 × ~14 スロット × 6 艇 ≈ 12,000 走点を 36 セル(4 級別 × 2〜3 グレード ×
6 コース、実質 (B1, B2 → 全)・(A1, A2 → SG_G1/G2_G3_一般))に分割すると 1 セル平均
200〜400 走点。コース由来の期待値・分散推定としては十分な精度。

### 4.3 フォールバック階層

`(class, grade, lane)` セルでサンプル不足の場合:

1. `(class, grade)` レベルの `(μ, σ)`(=コース無視の期待値・分散)
2. それも 5 サンプル未満なら **`(0, 1)`**(=実質コース補正なし、生得点をそのまま使用。
   σ=1 は単位を変えないため)

```python
def cell_stats(
    baseline: dict[tuple[str, str, int], tuple[float, float]],
    class_grade_avg: dict[tuple[str, str], tuple[float, float]],
    cls: str, grade: str, lane: int,
) -> tuple[float, float]:
    v = baseline.get((cls, grade, lane))
    if v is not None:
        return v
    v = class_grade_avg.get((cls, grade))
    if v is not None:
        return v
    return (0.0, 1.0)
```

### 4.4 算出タイミングとキャッシュ

- `compute_features_for_day()` 内で `load_motor_history()` 直後に 1 回算出。
- `FeatureContext` 経由の場合は **target_day 単位**でキャッシュする
  (window 内の各日で履歴が異なるため、`{day: (lane_baseline, class_grade_avg)}` の辞書)。
- `ENABLE_LANE_CORRECTION=False` のときは baseline 算出自体をスキップ(in-memory 集計
  であっても CPU を浪費しない)。

```python
# FeatureContext 拡張
self._lane_baseline_cache: dict[
    dt.date, tuple[dict[tuple[str, str, int], tuple[float, float]],
                   dict[tuple[str, str], tuple[float, float]]]
] = {}
```

---

## 5. スコアリング式(v2 motor_ability_pt)

### 5.1 数式

走 `i` について:

```
raw_i        = score_motor_run(table, run_i)[0]                 # 0 着なら -100, F/L 等なら None で除外
(μ_i, σ_i)   = cell_stats(..., run_i.cls, run_i.grade, run_i.lane)

# コース補正 + SD 標準化
residual_i   = (raw_i - μ_i) / σ_i        if ENABLE_LANE_CORRECTION
             = raw_i                       otherwise

# 時間減衰
days_ago_i   = max(0, (target_day - run_i.race_date).days)
w_i          = exp(- ln(2) × days_ago_i / 60)   if ENABLE_DECAY
             = 1.0                              otherwise

# 加重平均
Σw   = Σ w_i
Σwr  = Σ w_i × residual_i
Σw²  = Σ w_i²

n_eff      = (Σw)² / Σw²                 # Kish の有効サンプル数(全 w=1 なら n_eff=N)
mean_resid = Σwr / Σw                    # 加重平均(z)残差

# ベイズ収縮(prior 平均 0 へ縮める)
motor_ability_pt = n_eff / (n_eff + k) × mean_resid   if ENABLE_SHRINKAGE
                 = mean_resid                          otherwise
```

**v1 等価性**: `ENABLE_DECAY = ENABLE_LANE_CORRECTION = ENABLE_SHRINKAGE = False` かつ
`MOTOR_HISTORY_SESSIONS=5` で、`motor_ability_pt = Σraw / N`(単純平均)に縮退して
v1 と一致。これを §8.1 ユニットテストで保証する。

### 5.2 関数実装スケッチ

```python
def motor_ability_pt(
    history: dict[tuple[str, int], list[list[MotorRun]]],
    score_table: dict[tuple[str, str], list[int]],
    lane_baseline: dict[tuple[str, str, int], tuple[float, float]],
    class_grade_avg: dict[tuple[str, str], tuple[float, float]],
    stadium_code2: str, motor_num: int, target_day: dt.date,
) -> float:
    sessions = history.get((stadium_code2, motor_num))
    if not sessions:
        return float("nan")

    sum_w = 0.0
    sum_wr = 0.0
    sum_w2 = 0.0
    for sess in sessions:
        for run in sess:
            sc = score_motor_run(score_table, run)
            if sc is None:
                continue
            raw = float(sc[0])
            cls = run.racer_class
            bucket = run.grade_bucket if cls in ("A1", "A2") else "全"

            if ENABLE_LANE_CORRECTION:
                mu, sigma = cell_stats(lane_baseline, class_grade_avg,
                                       cls, bucket, run.lane)
                residual = (raw - mu) / sigma
            else:
                residual = raw

            if ENABLE_DECAY:
                days_ago = max(0, (target_day - run.race_date).days)
                w = math.exp(-DECAY_LAMBDA * days_ago)
            else:
                w = 1.0

            sum_w  += w
            sum_wr += w * residual
            sum_w2 += w * w

    if sum_w == 0.0:
        return float("nan")        # 下流の z 化で 50 補完
    mean_resid = sum_wr / sum_w
    if not ENABLE_SHRINKAGE:
        return mean_resid
    n_eff = (sum_w * sum_w) / sum_w2
    return n_eff / (n_eff + SHRINKAGE_PRIOR_K) * mean_resid
```

### 5.3 引数バンドル(将来検討)

シグネチャが 7 引数で重い(レビュー懸念 5)。フィーチャーフラグも増えるため、
将来的に `MotorAbilityContext` dataclass で `(history, score_table, lane_baseline,
class_grade_avg, target_day)` を束ねるリファクタが望ましい。今回は ablation テストの
互換性確保を優先し、現状シグネチャで実装する。

### 5.4 出力値域の挙動

- `mean_resid` の理論レンジ(全フラグ ON 時): 概ね `-3.0 〜 +3.0`(z 残差スケール)。
  事故 -100 を含む走でも σ ≈ 50 程度のセルでは `(-100 - 0)/50 = -2.0` 程度に収まる
- 収縮後はそれを `n_eff/(n_eff + 10)` でスケール → 多くは `-1.5 〜 +1.5` レンジ
- 平均(全モーターでの算術平均)は構造的に **ほぼ 0**(z 残差の和は baseline 算出元と
  同じ集合なら厳密 0、prior 収縮で僅かに 0 寄り)
- 下流の場別 z 化(`build_index.py` の `(x - μ_motor) / σ_motor × 10 + 50`)で
  最終的に 50/10 に揃う

**v1 との外形互換性**: 出力 CSV のスケール(平均 50・SD 10)は変わらない。
**重みファイル**(`stadium/index_weights/YYYY-MM.csv`)の **数値は変わる** ので
リリース時に再学習が必要(v1 のスケールよりノイズが減っているはずなので、
`motor` の係数自体は概ね同方向に動く想定)。

---

## 6. パイプライン統合

### 6.1 build_index.py(日次)変更点

```text
従来 v1:
  load_motor_period_starts(repo, day)           # period_starts
  load_motor_history(repo, day, period_starts)  # history
  for race: motor_ability_pt(history, table, stadium, motor_num)

v2:
  load_motor_period_starts(repo, day)           # period_starts (変更なし)
  load_motor_history(repo, day, period_starts)  # history(各 MotorRun に race_date/lane 追加)
  compute_lane_baseline(history.values())       # 動的 baseline(日次 1 回、24 場プール)
  compute_class_grade_avg(history.values())     # フォールバック用
  for race: motor_ability_pt(
      history, table, lane_baseline, class_grade_avg,
      stadium, motor_num, target_day=day)
```

`load_motor_history()` の負荷は v1 比で **微増**(`MOTOR_HISTORY_SESSIONS` を
5→6 に増やしたぶんだけ race_cards 読込が ~1.2 倍)。`compute_lane_baseline()` は
in-memory 集計だけなので無視可。

### 6.2 build_weights.py(月次)変更点

過去日ごとに `lane_baseline` を再計算する必要がある(履歴ウィンドウが日付依存)。
`FeatureContext` 側のキャッシュ層で `{day: lane_baseline}` をメモ化することで
window 全日に対する重複計算を避ける。

旧重みファイルは **互換性なし**。リリース時に対象月を `build_weights.py --month YYYY-MM`
で必ず再学習する(`mu_motor` / `sigma_motor` も v2 スケールに合わせて再フィット)。

### 6.3 FeatureContext 拡張

```python
class FeatureContext:
    # 既存に追加
    self._lane_baseline_cache: dict[
        dt.date, tuple[dict[tuple[str, str, int], tuple[float, float]],
                       dict[tuple[str, str], tuple[float, float]]]
    ] = {}

    def lane_baselines(
        self, day: dt.date,
    ) -> tuple[dict[tuple[str, str, int], tuple[float, float]],
               dict[tuple[str, str], tuple[float, float]]]:
        if day not in self._lane_baseline_cache:
            if not ENABLE_LANE_CORRECTION:
                self._lane_baseline_cache[day] = ({}, {})
                return self._lane_baseline_cache[day]
            history = self.motor_history(day)
            all_runs = [r for sess_list in history.values()
                          for sess in sess_list for r in sess]
            self._lane_baseline_cache[day] = (
                compute_lane_baseline(all_runs, self.motor_score_table()),
                compute_class_grade_avg(all_runs, self.motor_score_table()),
            )
        return self._lane_baseline_cache[day]
```

### 6.4 後方互換

| 観点 | 互換性 |
| --- | --- |
| 出力 CSV のスキーマ | ◯ 変更なし(`N枠_モーターpt` / `N枠_寄与_モーターpt` 列据え置き) |
| 出力 CSV の値スケール | ◯ 平均 50・SD 10(下流 z 化で吸収) |
| 重み CSV のフォーマット | ◯ 変更なし |
| 重み CSV の値 | ✕ 数値は変わる(再学習必須) |
| 旧 `motor_pt` 外部参照 | リリースノートで「同列名で定義変更(v1→v2)」を周知 |

---

## 7. フェイルセーフ(v2 固有部分)

v1 §5.4 のすべてに加え、v2 で新たに想定する異常系:

| 異常系 | 挙動 |
| --- | --- |
| `艇N_節D{D}走{S}_進入` も `_枠` も欠損 | その走をスキップ(分母にも乗らない) |
| `_進入` が "0" / "7" 等の不正値 | `_枠` にフォールバック、それも不正なら走をスキップ |
| `(class, grade, lane)` セルが 5 サンプル未満 | `(class, grade)` フォールバック |
| `(class, grade)` も 5 サンプル未満 | `(μ, σ) = (0, 1)` で実質コース補正なし |
| `σ_cell ≈ 0`(セル内全走同点の退化ケース) | `σ_floor = 10` に丸める |
| `target_day < race_date`(時計巻き戻し) | `days_ago = max(0, …)` で 0 扱い(=重み 1.0) |
| 履歴に全走存在するが全て F/L 等(score_motor_run=None) | `motor_ability_pt = NaN` → 50 補完 |
| 節日数が 8 日以上(理論的) | slot D が 7 までしか無いので問題なし。session_dates が 8 日以上でも `[D-1]` 参照は安全 |
| session_dates のインデクスが slot D を超える | `race_date = session_end` にフォールバック |
| 節中に中止日 1 日含むケース | `detect_session_end_days` の連続日まとめロジックで同節扱い(日差 ≤ 1)。slot D の `race_date` が実日付から 1 日ずれるが減衰重みへの影響は exp(-ln2/60) ≈ 0.99 倍で実害なし |
| `ENABLE_LANE_CORRECTION=False` | baseline 算出スキップ、`residual = raw` |
| `ENABLE_DECAY=False` | `w_i = 1.0`、`n_eff = N`(集計対象走数) |
| `ENABLE_SHRINKAGE=False` | `motor_pt = mean_resid`(収縮なし) |

---

## 8. テスト計画

### 8.1 ユニットテスト(`scripts/tests/unit/test_motor_ability_v2.py` 新規)

- `parse_lane()` 真理表
  - `(進入="3", 枠="1") → 3`(進入優先)
  - `(進入="", 枠="2") → 2`(枠フォールバック)
  - `(進入="0", 枠="6") → 6`(不正進入はスキップ)
  - `(進入=None, 枠=None) → None`
- `compute_lane_baseline()` の集計
  - モックの MotorRun リストを与え、各セルの `(μ, σ)` が期待値と一致(母集団 SD)
  - サンプル数 < min_samples のセルが結果から欠落していること
  - F/L/失/妨 を含む走が分母に乗らないこと
  - `σ < sd_floor` のセルが `σ_floor = 10` に丸められること
- `compute_class_grade_avg()` のフォールバック集計確認
- `cell_stats()` のフォールバック階層
  - lane あり → `(μ_lane, σ_lane)` 返却
  - lane なし、class_grade あり → `(μ_cg, σ_cg)` 返却
  - 両方なし → `(0, 1)` 返却
- 時間減衰の重み
  - `days_ago = 0 → w = 1.0`
  - `days_ago = 60 → w ≈ 0.5`
  - `days_ago = 120 → w ≈ 0.25`
- `n_eff` の計算
  - 全 weight 同値 `w_i = 1.0`, n=10 → `n_eff = 10.0`
  - 加重 `w = [1,1,1,1,1, 0.5,0.5,0.5,0.5,0.5]` → `Σw=7.5, Σw²=6.25, n_eff = 56.25/6.25 = 9.0`
- 収縮式
  - `n_eff = 10, k = 10, mean_resid = +1.0 → 出力 +0.5`(50% 収縮)
  - `n_eff = 100, k = 10, mean_resid = +1.0 → 出力 +1.0 × 100/110 ≈ 0.909`
- `motor_ability_pt()` end-to-end
  - 2 節分 10 走の手組み MotorRun を与えて、z 残差の加重平均と収縮結果が手計算と一致
- **v1 算術等価性(重要・段階リリース基盤)**
  - `ENABLE_DECAY = ENABLE_LANE_CORRECTION = ENABLE_SHRINKAGE = False` +
    `MOTOR_HISTORY_SESSIONS = 5` で `motor_ability_pt` が v1 実装と float epsilon
    以内で一致することを assertion(同一データを v1/v2 両関数に通す)
- フィーチャーフラグ個別 ON 検証
  - DECAY のみ ON → 加重平均生得点
  - LANE のみ ON → 単純平均 z 残差(等加重)
  - SHRINKAGE のみ ON → 単純平均生得点を n/(n+k) 倍

### 8.2 統合テスト

- `compute_features_for_day(repo, 既知の日)` を呼び、`motor` 列が NaN 過多でないこと
  - **定量閾値**: `motor` が NaN のレース行が全レース行の 5% 未満
- **FeatureContext マルチデイ呼び出しテスト**
  - 連続 3 日分の `compute_features_for_day` を 1 つの Context で実行
  - `_lane_baseline_cache` のキャッシュヒット率が 2/3 以上(初回 miss 後はヒット)
  - 同じ日について Context 経由と直接呼び出しで `motor` 列が一致
- **スナップショットテスト**
  - 既知の代表日(例: 2026-05-01)で v2 の `motor` 列を保存(`tests/fixtures/motor_pt_v2_snapshot.csv`)
  - リファクタやパラメータ調整後にスナップショット差分が許容範囲内であることを確認
- **パフォーマンス回帰テスト**
  - 1 日分 `build_index.py --mode daily` の実行時間を測定
  - v1 比 1.5 倍以内(目安: 6 節走査 + baseline 算出のオーバーヘッド許容)
- v1 vs v2 の同日比較スクリプト `scripts/tests/compare_motor_v1_v2.py`
  - 同じ日の motor_pt(v1) と motor(v2) を散布図化、r² と SD を比較
  - 「コース補正で 1 コース勝ちの A1 が以前より弱くなる/6 コース勝ちの A1 が強くなる」
    という質的検証

### 8.3 重み回帰テスト(ablation 含む)

- 同月の重みを 4 構成で個別学習し、`tmp/motor_v2_ablation.csv` に出力:
  | バージョン | DECAY | LANE | SHRINKAGE | N_SESSIONS |
  | --- | --- | --- | --- | --- |
  | v1_baseline | False | False | False | 5 |
  | v2_decay_only | True | False | False | 6 |
  | v2_decay_lane | True | True | False | 6 |
  | v2_full | True | True | True | 6 |
- 出力列: `month, variant, motor_coef, motor_r2_partial, total_r2`
- 各 variant の差分から「どの仕掛けが総 r² を何ポイント押し上げたか」を切り分け

### 8.4 リリース判断基準(success criteria 双方向)

| 判定 | 基準 | アクション |
| --- | --- | --- |
| **GO(積極)** | 直近 6 ヶ月平均で `total_r²(v2_full) − total_r²(v1)` ≥ **+0.3pp** | リリース |
| **GO(消極)** | 平均差が `[-0.2, +0.3) pp` で **どの単月も -0.5pp 未満ない** | リリース(理論的整合性で押し切る) |
| **STOP(再検討)** | 平均差 `< -0.2 pp`、または任意の単月で `< -0.5 pp` | 原因分析。ablation 表で犯人特定 → 該当フラグを `False` で再評価 |
| **STOP(致命)** | 任意の単月で `< -1.0 pp` または NaN 率 5% 超 | 設計の前提見直し |

---

## 9. ドキュメント更新箇所(CLAUDE.md ルール準拠)

| 更新ファイル | 内容 |
| --- | --- |
| `docs/data/estimate.md` | `N枠_モーターpt` の説明を v2(残差 × 減衰 × 収縮)に改稿 |
| `docs/data/motor_ability_score.md` | スコアテーブル CSV の役割を「生得点の元表」と再定義 |
| `docs/development.md` | `scripts/build_index.py` の依存に baseline 動的算出を追記 |
| `docs/design/motor_ability_index.md` | 冒頭に「**v1 設計書(現状)。v2 については `motor_ability_index_v2.md` を参照**」を追記 |
| `docs/design/motor_ability_index_v2.md` | 本ファイル |

---

## 10. リリース手順

### 10.1 事前 EDA(実装着手前)

- `_進入` と `_枠` の一致率を直近 3 ヶ月分の race_cards で集計
  (`scripts/tests/inspect_shinnyu_waku.py` 新規、出力 `tmp/shinnyu_waku_mismatch.csv`)
  - 一致率が 95% 未満なら設計仮定(=進入優先)を再検討
  - グレード別・場別の偏りも確認
- 既存履歴データで `(class, grade, lane)` セルの想定サンプル数分布を確認
  - 全 36 セルのうち `min_samples=5` 未満になるセルがあれば、フォールバック頻度を見積もる

### 10.2 実装

1. `scripts/boatrace/index_features.py` 改修
   - `MotorRun` に `race_date` / `lane` 追加
   - `detect_sessions()` 新設、`detect_session_end_days()` をラッパ化
   - `extract_runs_for_session()` で `_進入` / `_枠` を読み出して `lane` 設定、
     `race_date` を `session_dates[D-1]` で設定
   - `compute_lane_baseline()` / `compute_class_grade_avg()` / `cell_stats()` /
     `parse_lane()` 新設
   - `motor_ability_pt()` を v2 数式に差し替え(引数追加 + フラグ分岐)
   - 定数群を §2.3 に従い更新(`ENABLE_*` フラグ追加、`MOTOR_HISTORY_SESSIONS=6`)
2. `FeatureContext.lane_baselines()` 追加、`compute_features_for_day()` の呼び出し更新

### 10.3 テスト

3. `scripts/tests/unit/test_motor_ability_v2.py` 追加、`pytest scripts/tests/unit/` 全 PASS
   - **特に「v1 算術等価性」テスト(全フラグ OFF + N=5)が必ず PASS であること**
4. `scripts/tests/compare_motor_v1_v2.py` で v1/v2 比較。`r²` 改善方向を確認
5. ablation 実行: 4 構成で `scripts/tests/run_ablation.py --month YYYY-MM`
   - §8.4 の success criteria を満たすか確認

### 10.4 リリース

6. リリース対象月の重みを `python scripts/build_weights.py --month YYYY-MM` で再学習
7. 当日 index を `python scripts/build_index.py --mode daily --date $TODAY_JST --force` で
   再生成、`N枠_モーターpt` の分布(平均 50・SD 10)を目視確認
8. `docs/` 一式を同 PR で更新(CLAUDE.md ルール)
9. 旧 v1 の `motor_ability_pt` シグネチャ参照が呼び出し側(`build_index.py` 含む)に
   残っていないことを grep で確認

### 10.5 リリース後モニタリング

`index` 監視ダッシュボード(別途存在)に以下を追加:

| 指標 | 観点 | アラート閾値 |
| --- | --- | --- |
| 場別 `motor` 列の日次平均 | 50 から大きく外れる場の検知 | `\|mean - 50\| > 2.0` が 3 日連続 |
| 場別 `motor` 列の日次 SD | SD 10 から大きく外れる場の検知 | `\|SD - 10\| > 2.0` が 3 日連続 |
| `motor` NaN 率 | データ取りこぼし検知 | 5% 超 |
| 期切替直後モーターの `motor` 分布 | 収縮が効いているか(50 近辺集中が期待) | 中央値が `[45, 55]` を外れる |
| baseline 算出時の `(class, grade, lane)` 全 36 セル数 | フォールバック発生検知 | サンプル不足セル数が 3 以上 |

---

## 11. 将来課題

- **半減期の場別最適化**: 場ごとに整備頻度・節間隔が違うので `H` を 30〜120 日で
  場別グリッドサーチする余地あり
- **`進入` 情報の信頼度フラグ**: スタート展示の進入と本番進入が違うケースの取扱い。
  本設計は「`_進入` 列の値」をそのまま信頼している
- **prior 平均を 0 以外に**: 物理的に新モーターが弱い傾向があるならグローバル平均より
  下にずらすことで保守的に。データ駆動で `μ₀` をフィットする選択肢あり
- **モーター×場 相性**: 現状 baseline はコース×級別×グレードのみ。「特定の場で
  はね出やすい」モーター特性を分離するには baseline に場を加える(セル細分化で
  サンプル数不足リスク)
- **負スコア -100 のキャリブレーション**: v1 §9 から継続課題。z 残差化したことで
  影響度が σ_cell に依存するので、当初想定と挙動が変わる可能性あり
- **コース baseline のリーケージ削減**: 各モーター自身の走が baseline に寄与する循環
  参照を leave-one-motor-out などで除去する余地あり(実装重・効果限定的)
- **`MotorAbilityContext` への引数バンドル**: §5.3 参照
- **フィーチャーフラグの config 化**: `.boatrace/config.json` から読み込めるようにし、
  場別 / 月別の細かい切替も可能にする(現状は定数のソースコード書き換え)
