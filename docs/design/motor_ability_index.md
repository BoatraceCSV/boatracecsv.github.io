# モーター能力指数(Motor Ability Index)設計書

> **⚠️ 本書は v1 設計です。現行実装は v2(時間減衰 + コース補正 + ベイズ収縮)に
> 切り替わっています。新規参照は [`motor_ability_index_v2.md`](./motor_ability_index_v2.md)
> を参照してください。** v1 の核(スコアテーブル CSV / 着順トークン分類 / 期境界
> フィルタなど)は v2 にも継承されているので、本書の §2 / §3.6 / §3.7 / §3.10 は
> 引き続き有効。

選手ptと同じ「能力指数 → 場別 z スコア」の枠組みでモーター強度を再定義する。
現行の `モーターpt = 場別 z(モーター勝率)` を、**直近 5 節の出走実績を**
**級別 × グレード重みで得点化した平均値**の場別 z スコアに置き換える。

- 対象スクリプト: `scripts/boatrace/index_features.py`
- 対象データ生成: `scripts/build_index.py` / `scripts/build_weights.py`
- 対象出力列: `data/estimate/index/YYYY/MM/DD.csv` の `N枠_モーターpt`(意味のみ変更、列名据え置き)
- 影響範囲: 重み学習(`build_weights.py`)、月次重み CSV、Strength Index、index 監視ダッシュボード

---

## 1. 確定パラメータ

| 項目 | 値 | 備考 |
| --- | --- | --- |
| (a) スコアテーブルの持ち方 | **CSV ファイル** | `data/estimate/motor_ability_score.csv` に外出し |
| (b) 直近 N 節 | **5 節** | 選手pt(recent_form と同じ)と揃える |
| (c) 欠損時の扱い | **NaN → 50 補完(既存ルール踏襲)** | データ不足モーターは平均扱い |

---

## 2. スコアテーブル CSV 設計

### 2.1 ファイル

- パス: `data/estimate/motor_ability_score.csv`
- 文字コード: UTF-8 (BOM なし)
- 行数: 6 行(級別 × グレード分類の組み合わせ)
- 1着〜6着 6 列で得点を持つ wide 形式

### 2.2 スキーマ

| 列 | 型 | 説明 |
| --- | --- | --- |
| `級別` | str | `B2` / `B1` / `A2` / `A1` |
| `グレード分類` | str | `全`(B2/B1)/ `SG_G1` / `G2_G3_一般`(A2/A1)|
| `1着pt` 〜 `6着pt` | int | 各着順の得点 |

### 2.3 内容(ユーザー指定値)

```csv
級別,グレード分類,1着pt,2着pt,3着pt,4着pt,5着pt,6着pt
B2,全,125,100,75,50,25,0
B1,全,100,80,60,40,20,0
A2,SG_G1,125,100,75,50,25,0
A2,G2_G3_一般,75,60,45,30,15,0
A1,SG_G1,100,80,60,40,20,0
A1,G2_G3_一般,50,40,30,20,10,0
```

### 2.4 グレード分類の判定ルール

`title` CSV の `グレード` 列(`IP` / `G3` / `G2` / `G1` / `SG` / `PG1` 等)を以下に正規化する。
既存の `index_features.grade_of()` を一部流用して新関数 `grade_bucket_for_motor()` を追加する。

| グレード原値 | 分類 |
| --- | --- |
| `SG` / `ＳＧ` / `PG1` / `ＰＧ１` / `G1` / `Ｇ１` / `ＧⅠ` | `SG_G1` |
| `G2` / `Ｇ２` / `ＧⅡ` / `G3` / `Ｇ３` / `ＧⅢ` / `IP`(一般) / その他 | `G2_G3_一般` |

A1 / A2 で `SG_G1` ⇔ `G2_G3_一般` を切り替え、B1 / B2 では分類を見ず `全` を採用する。

### 2.5 失格・棄権の扱い(モーター固有ルール)

選手pt は「機材事故は選手の責任ではないので除外」だが、モーターptは逆に
「機材起因の事故はモーターの欠陥/トラブルを示唆するためマイナス点」とし、
「選手起因の失格はモーター評価から除外」とする(ユーザー指定)。

| 着順トークン | スコア化 | 分母(出走数) |
| --- | --- | --- |
| `1`〜`6`(半角・全角) | テーブル値を加算 | +1 |
| `転` / `落` / `沈` / `エ` | **-100 点として加算** | +1 |
| `F` / `L` / `失` / `妨`(選手起因) | スキップ(モーター評価対象外) | +0 |
| `欠` / `不` | スキップ(無効走) | +0 |

意図:
- `転`(転覆) / `落`(落水) / `沈`(沈没) / `エ`(エンスト) は機材の欠陥・整備不良・
  出力異常が主因となりうるため、**モーター評価としてマイナス計上**(-100)。
  事故 1 回が 1 着 1 回分(B2 級で +125)に近いインパクトで効くスケール。
- `F`(フライング) / `L`(出遅れ) / `失`(失格) / `妨`(妨害失格) は **選手起因**
  なのでモーター評価には乗せない(分子にも分母にも加えない)。選手pt 側で 0 点として
  反映されるロジックとは非対称になるが、これは設計意図どおり。
- `欠`(欠場) / `不`(不完走) は走行自体が成立していないため除外。

> 選手pt との非対称性を意図的に作る箇所なので、`scripts/boatrace/index_features.py`
> に以下の定数を明示的に置く:
>
> ```python
> MOTOR_NEGATIVE_TOKENS = {"転", "落", "沈", "エ"}
> MOTOR_NEGATIVE_SCORE = -100
> MOTOR_SKIP_TOKENS    = {"F", "L", "失", "妨", "欠", "不"}
> ```

---

## 3. データソースと履歴ビルダー

### 3.1 データソース選定の決定

**転/落 を判別するため、`data/results/realtime/` ではなく `data/programs/race_cards/`
の「節間14スロット成績」(`艇N_節D{D}走{S}_着順`)を主データソースとする**。

`results/realtime` には `F` フラグはあるが `L` / `欠` / `転` / `落` を直接区別する
列が無いため、ユーザー要件「転/落 = -20」を満たせない。一方 race_cards 14 スロットの
`着順` 列はソース時点で全トークン(`1`〜`6` / `F` / `L` / `欠` / `転` / `妨` / `落` /
`エ` / `不` / `沈` / `失`)を保持している。

### 3.2 入力データ

| ソース | 取得項目 | 利用可能開始日 |
| --- | --- | --- |
| `data/programs/race_cards/YYYY/MM/DD.csv` | `艇N_モーター番号`, `艇N_級別`, `艇N_節D{D}走{S}_着順` | 2025-05-03 |
| `data/programs/title/YYYY/MM/DD.csv` | `グレード` | 2026-05-01 |
| `data/programs/motor_stats/YYYY/MM/DD.csv` | `モーター期起算日`(履歴リセット境界) | 2026-04-25 |

> **`results/realtime` は使用しない**。スコアテーブルの照合に必要な全情報が
> race_cards + title + motor_stats で揃う。

> **重要: `モーター期起算日` で履歴リセット**。同じ `(場, モーター番号)` でも
> モーター交換・期切替が行われると物理的に別個のモーターとなる。
> モーター期起算日より前の節は履歴から除外する(§3.10 参照)。

### 3.3 1走レコードの定義

```python
@dataclass
class MotorRun:
    session_end: dt.date  # この走を含む節の最終開催日(節キー)
    stadium: str          # "01"〜"24"
    motor_num: int        # 物理モーター番号
    grade_bucket: str     # "SG_G1" / "G2_G3_一般" / "全"
    racer_class: str      # "A1" / "A2" / "B1" / "B2"
    finish: str           # スロットの 着順 トークンそのまま(全角→半角正規化済)
```

1 節分の race_cards 1 行 × 1 艇から、最大 14 個の MotorRun が生成される(14 スロット
中の埋まっているものすべて)。同じモーターが同じ節で重複抽出されるのを避けるため
**節最終日の race_cards 1 件のみ**を採用する(同一モーター × 同一節を 1 度しか読まない)。

### 3.4 節境界の検出

```python
def detect_session_end_days(repo: Path, stadium: str, window_end: dt.date,
                             window_days: int = 90) -> list[dt.date]:
    """
    場 stadium の直近 5 節分の「節最終日」を新→旧の順で返す。
    開催日は race_cards/YYYY/MM/DD.csv の存在 + 当該場のレースコードが含まれるかで判定。
    連続開催日(日差 1 日)を 1 節として束ねる。
    window_end は除外(当日を含む節は計算対象から外す)。
    """
    open_days: list[dt.date] = []
    for back in range(1, window_days + 1):
        d = window_end - dt.timedelta(days=back)
        if has_races_at(repo, d, stadium):   # race_cards/D.csv に当該 stadium 行ある?
            open_days.append(d)
    open_days.sort()                          # 古→新

    sessions: list[list[dt.date]] = []
    cur: list[dt.date] = []
    for d in open_days:
        if not cur or (d - cur[-1]).days <= 1:
            cur.append(d)
        else:
            sessions.append(cur); cur = [d]
    if cur:
        sessions.append(cur)

    last_days = [s[-1] for s in sessions]    # 各節の最終日
    return last_days[-5:][::-1]               # 直近 5 節を新→旧
```

`has_races_at(repo, d, stadium)`:
- `data/programs/race_cards/d.csv` を開き、`レースコード[8:10] == stadium` の行が
  1 つでもあれば True

### 3.5 1 節分の MotorRun 抽出

```python
def extract_runs_for_session(repo: Path, stadium: str, session_end: dt.date,
                              score_table) -> list[MotorRun]:
    rc = read_race_cards(repo, session_end)
    tt = read_title(repo, session_end)        # 同節内の任意日で OK だが最終日が常に存在

    # 当該節当該場のグレード(節内一定)
    grade_bucket = "G2_G3_一般"
    for _, row in tt.iterrows():
        if str(row["レースコード"])[8:10] == stadium:
            grade_bucket = grade_bucket_for_grade(row.get("グレード", ""))
            break

    # 当該場のレースコードを 1 つだけ拾えば 6 艇 × 14 スロット = 全モーター情報が取れる
    # ただし「節最終日に出走しないモーター」(節途中で代替モーター登場など)は
    # 取りこぼす可能性があるため、当該場の全レースを舐めて (motor_num → row) 辞書を作る
    motor_rows: dict[int, tuple[str, dict]] = {}   # {motor_num: (racer_class, slot_row)}
    for _, row in rc.iterrows():
        if str(row["レースコード"])[8:10] != stadium:
            continue
        for n in range(1, 7):
            motor_num = parse_int(row.get(f"艇{n}_モーター番号"))
            racer_class = (row.get(f"艇{n}_級別") or "").strip()
            if motor_num is None or not racer_class:
                continue
            if motor_num in motor_rows:
                continue              # 既に取得済(同一節内の同一モーターは同じ履歴)
            motor_rows[motor_num] = (racer_class, {
                f"D{d}走{s}_着順": row.get(f"艇{n}_節D{d}走{s}_着順")
                for d in range(1, 8) for s in (1, 2)
            })

    runs: list[MotorRun] = []
    for motor_num, (racer_class, slots) in motor_rows.items():
        eff_bucket = resolve_grade_bucket(racer_class, grade_bucket)
        for d in range(1, 8):
            for s in (1, 2):
                token = normalize_token(slots[f"D{d}走{s}_着順"])
                if token is None:
                    continue          # 未出走スロット
                runs.append(MotorRun(
                    session_end=session_end, stadium=stadium,
                    motor_num=motor_num, grade_bucket=eff_bucket,
                    racer_class=racer_class, finish=token,
                ))
    return runs
```

### 3.6 トークン正規化

```python
ZEN_TO_HAN_DIGIT = {"１":"1","２":"2","３":"3","４":"4","５":"5","６":"6"}
VALID_TOKENS = set("123456") | {"F","L","欠","転","妨","落","エ","不","沈","失"}

def normalize_token(raw) -> str | None:
    if raw is None: return None
    s = str(raw).strip()
    if not s or s.lower() == "nan": return None
    if s in ZEN_TO_HAN_DIGIT: return ZEN_TO_HAN_DIGIT[s]
    # 数値("4.0" 等)
    try:
        i = int(float(s))
        if 1 <= i <= 6: return str(i)
    except ValueError:
        pass
    # 全角 F/L → 半角(race-card scraper で正規化済のはずだが念のため)
    s = s.replace("Ｆ", "F").replace("Ｌ", "L")
    return s if s in VALID_TOKENS else None
```

### 3.7 グレード分類ヘルパ

```python
def grade_bucket_for_grade(grade_raw: str) -> str:
    s = (grade_raw or "").strip()
    if any(t in s for t in ("SG","ＳＧ","PG","ＰＧ","G1","Ｇ１","ＧⅠ")):
        return "SG_G1"
    return "G2_G3_一般"

def resolve_grade_bucket(racer_class: str, race_grade_bucket: str) -> str:
    if racer_class in ("B1", "B2"):
        return "全"
    return race_grade_bucket   # A1/A2 → SG_G1 or G2_G3_一般
```

### 3.8 履歴ローダ(全場横断)

```python
def load_motor_history(repo: Path, target_day: dt.date,
                        score_table) -> dict[tuple[str, int], list[list[MotorRun]]]:
    """
    Returns: {(stadium, motor_num): [節1分 MotorRun[], 節2分, ...]} を新→旧で。
    各リストが 1 節分。最大 5 要素。モーター期起算日より前の節は除外。
    """
    period_starts = load_motor_period_starts(repo, target_day)   # §3.10
    out: dict[tuple[str, int], list[list[MotorRun]]] = defaultdict(list)
    for stadium in STADIUM_NAMES:                          # "01".."24"
        # まず多めに節を集める(リセットで削られた分を補うため 10 節遡る)
        session_ends = detect_session_end_days(repo, stadium, target_day, max_sessions=10)
        per_motor_sessions: dict[int, list[list[MotorRun]]] = defaultdict(list)
        for sess_end in session_ends:                      # 新→旧
            sess_runs = extract_runs_for_session(repo, stadium, sess_end, score_table)
            grouped: dict[int, list[MotorRun]] = defaultdict(list)
            for r in sess_runs:
                grouped[r.motor_num].append(r)
            for motor_num, runs in grouped.items():
                per_motor_sessions[motor_num].append(runs)

        # モーター期起算日より前の節を切り捨て、最大 5 節に絞る
        for motor_num, sessions in per_motor_sessions.items():
            period_start = period_starts.get((stadium, motor_num))
            if period_start is not None:
                sessions = [s for s in sessions
                            if s and s[0].session_end >= period_start]
            out[(stadium, motor_num)] = sessions[:5]       # 新→旧で 5 件まで
    return out
```

### 3.9 設計上の重要判断

1. **節境界は「同じ場の連続開催日(差 1 日)」で機械的に検出**。
   title の `日次` 列は title 不在の過去日に使えないため信頼基盤にしない。
2. **target_day(算出対象日)は含めない**: 当日のレースは未来情報。
3. **同一節内で同一モーターを複数行から重複取得しない**: motor_rows 辞書で先勝ち。
   級別が節内で変わることはほぼ無く、変わっても先頭レース時点の値を採用する。
4. **節境界キー = `session_end`(節最終日の date)**。
5. **`艇N_節D{D}走{S}_着順` の最終日カバレッジ**: 当該節最終日の race_cards に
   その日のレース結果が反映されるかどうかは scraper の取得タイミング依存。
   通常は朝公開 → 当日中の自動再取得で 14 スロット中の最終 1〜2 スロットも午後には
   埋まる。最悪でも最終 2 スロット(≒ 14% 程度)欠損だが、z スコア化で吸収される。
6. **モーター期境界での履歴リセット**: §3.10 で詳述。同一場・同一番号でも期が違う
   モーターは別物として扱う。

### 3.10 モーター期起算日テーブル

```python
def load_motor_period_starts(repo: Path, target_day: dt.date,
                              fallback_days: int = 14) -> dict[tuple[str, int], dt.date]:
    """
    Returns: {(場コード2桁, モーター番号): モーター期起算日}.

    target_day の motor_stats が無ければ、最大 fallback_days 日遡って探索する。
    motor_stats は当日開催のある場のみを収録するため、場ごとに「直近で得られた
    スナップショット」をマージする(休場日の影響を吸収)。
    """
    out: dict[tuple[str, int], dt.date] = {}
    seen_stadiums: set[str] = set()                # 場ごとに最新スナップショットだけ採用

    for back in range(0, fallback_days + 1):       # target_day を含む(当日 motor_stats も使う)
        d = target_day - dt.timedelta(days=back)
        p = (repo / "data" / "programs" / "motor_stats"
             / f"{d:%Y}" / f"{d:%m}" / f"{d:%d}.csv")
        if not p.exists():
            continue
        df = pd.read_csv(p, dtype=str)
        for _, row in df.iterrows():
            stadium = str(row["場コード"]).zfill(2)
            if stadium in seen_stadiums:
                continue                            # 既により新しいスナップショットを採用済み
            try:
                num = int(float(row["モーター番号"]))
                start = dt.date.fromisoformat(row["モーター期起算日"])
            except (ValueError, TypeError, KeyError):
                continue
            key = (stadium, num)
            if key not in out:
                out[key] = start
        # この日付の場をまとめて seen 化(同一日内の他モーターも採用済とみなす)
        for stadium in df["場コード"].dropna().astype(str).str.zfill(2).unique():
            seen_stadiums.add(stadium)
    return out
```

**フィルタ仕様**:

- `(場, モーター番号)` に対する `モーター期起算日` が見つかった場合: 節最終日が
  `モーター期起算日` **以降** の節だけを残す(`session_end >= period_start`)。
- `モーター期起算日` が見つからない場合(古いモーター・休場長期化など): フィルタ
  しない(=全 5 節を採用)。フェイルセーフとして妥協。
- モーター期境界が節の途中(中 1 日とは別の理由で)に来るケースは事実上発生しない
  (期切替は節と節の間に行われる)。理論上の straddle は無視する。
- 期切替直後で 0 節しか集まらない新モーターは `motor_ability_pt = NaN` → 50 補完。
  これは「新モーターは平均扱い」という直感に合致。

---

## 4. スコアリングと motor_pt 関数差し替え

### 4.1 スコアテーブル参照

```python
MOTOR_NEGATIVE_TOKENS = {"転", "落", "沈", "エ"}  # 機材起因 → -20 点で打点
MOTOR_NEGATIVE_SCORE  = -100
MOTOR_SKIP_TOKENS     = {"F", "L", "失", "妨", "欠", "不"}  # 選手起因 + 無効走 → 集計除外


def load_score_table(repo: Path) -> dict[tuple[str, str], list[int]]:
    df = pd.read_csv(repo / "data" / "estimate" / "motor_ability_score.csv")
    return {(row["級別"], row["グレード分類"]):
            [int(row[f"{k}着pt"]) for k in range(1, 7)]
            for _, row in df.iterrows()}


def score_motor_run(table, run: MotorRun) -> tuple[int, int] | None:
    """
    Returns (得点, 分母+1) or None (=分母にも乗らない).
    """
    key = (run.racer_class, run.grade_bucket if run.racer_class in ("A1", "A2") else "全")
    pts = table.get(key)
    if pts is None:
        return None
    f = run.finish
    if f in ("1", "2", "3", "4", "5", "6"):
        return pts[int(f) - 1], 1
    if f in MOTOR_NEGATIVE_TOKENS:
        return MOTOR_NEGATIVE_SCORE, 1            # 転 / 落 / 沈 / エ → -20
    if f in MOTOR_SKIP_TOKENS:
        return None                                # F / L / 失 / 妨 / 欠 / 不 → ノーカウント
    return None                                    # 未知トークンは安全側でスキップ
```

### 4.2 1 モーターの能力指数

```python
def motor_ability_pt(history, table, stadium_code2: str, motor_num: int) -> float:
    sessions = history.get((stadium_code2, motor_num))
    if not sessions:
        return float("nan")     # 後段で 50 補完
    total_pt, total_runs = 0, 0
    for sess in sessions:        # 最大 5 節
        for run in sess:
            r = score_motor_run(table, run)
            if r is None:
                continue
            total_pt += r[0]
            total_runs += r[1]
    if total_runs == 0:
        return float("nan")
    return total_pt / total_runs
```

### 4.3 既存 `motor_pt()` / `load_motor_table_for_day()` の廃止

- 旧 `load_motor_table_for_day()` / `motor_pt()` は **完全に削除する**(リネームも残置もしない)。
- `compute_features_for_day()` の `mpt = motor_pt(...)` 呼び出し箇所は
  `mpt = motor_ability_pt(history, score_table, stadium_code2, m_num)` に差し替え。
- `motor_ability_pt` が NaN を返した場合、既存ロジックどおり `build_index.py` 側で z
  スコア計算時に `mu_motor` 補完(=偏差値 50)が適用される。
- 削除に伴い `data/programs/motor_stats/` への依存が `index_features.py` から消えるが、
  `motor_stats` CSV 自体の生成・配信は継続(他用途で参照されている可能性があるため)。
- リポジトリ全体に対し `grep -rn "load_motor_table_for_day\|motor_pt(" scripts/ docs/`
  で参照ゼロを確認してからマージする。

---

## 5. パイプライン統合

### 5.1 build_index.py(日次)

```text
従来:
  load_motor_table_for_day(day)         ─→ table (場×モーター → 勝率)
  for race: motor_pt(table, ...)

新規:
  load_score_table()                    ─→ score_table
  load_motor_period_starts(repo, day)   ─→ period_starts (場×モーター → モーター期起算日)
  load_motor_history(repo, day, score_table)
                                        ─→ history (24場×直近10節を走査、期境界で剪定後5節)
  for race: motor_ability_pt(history, score_table, stadium_code2, motor_num)
```

- 履歴ロードは **1 日 1 回 / 全レース共通**。24 場 × 最大 10 節最終日 = 最大 240 ファイルの
  race_cards を読む。1 ファイル = 12 〜 24 レース × 6 艇 = 100〜150 行。pandas で数秒。
- 履歴キャッシュ(`data/estimate/motor_history/YYYY-MM-DD.parquet`)は **第二段階で検討**。
  まずは毎回再構築で十分。
- 各場の節検出は最大 90 日遡及するため、race_cards/title が古い日付に欠けていても
  「見つかった分だけで 5 節まで」採用する。
- 期境界で剪定した結果 5 節未満になるケースは正常動作(新モーターは平均扱いが妥当)。

### 5.2 build_weights.py(月次)

- 過去 6 ヶ月の Strength Index 学習データを再生成する箇所で `motor` 特徴量だけ
  新ロジックに差し替える。学習窓内の過去日についても `load_motor_history(repo, day)`
  を当該日基準で呼び直す必要がある(履歴は時点依存)。
- `mu_motor` / `sigma_motor` は再学習されるため、自動で新指標スケールに合う。
- 既存重みファイル `data/estimate/stadium/index_weights/YYYY-MM.csv` のフォーマットは
  変更不要。**ただし重みの数値は変わる**ので、リリース時に当該月の重みを再生成すること。

### 5.3 後方互換

- 出力 CSV(`data/estimate/index/YYYY/MM/DD.csv`)のスキーマは変更なし。
  `N枠_モーターpt` / `N枠_寄与_モーターpt` の意味だけが変わる。
- 重みファイルのフォーマットも変更なし。
- 旧 `モーターpt` を参照していた外部ユーザがいる場合に備え、リリースノートで
  「同じ列名のまま定義変更」を明記する(`docs/data/estimate.md` のテキストを更新)。

### 5.4 フェイルセーフ

| 異常系 | 挙動 |
| --- | --- |
| `motor_ability_score.csv` が存在しない | `RuntimeError` で build_index 失敗(検知重視) |
| 過去 90 日に該当モーターが出走 0 回 | `motor_ability_pt = NaN` → z 化で 50 補完 |
| `title` が完全に無い日(2026-04 以前) | 全レース `G2_G3_一般` 扱い |
| `race_cards` の `艇N_級別` が空欄(極稀) | その走をスキップ |
| 14 スロット中、節最終日の最終 1〜2 スロットが未充填 | スキップ(集計から欠落)。z 化で吸収 |
| `艇N_モーター番号` が数値化できない | その走をスキップ |
| 未知の `着順` トークン | スキップ(将来トークン追加に備えた安全側設計) |
| `motor_stats` が直近 14 日に存在しない場のモーター | 期境界フィルタを適用せず全 5 節採用(妥協) |
| 期切替直後で 0 節しか集まらないモーター | `motor_ability_pt = NaN` → z 化で 50 補完(新モーター=平均扱い) |

---

## 6. テスト計画

### 6.1 ユニットテスト(`scripts/tests/test_motor_ability.py`)

- `load_score_table()` の戻り値が 6 キー × 6 整数になっていること
- `resolve_grade_bucket()` 真理表: (A1, SG_G1) / (A1, G2_G3_一般) / (A2, SG_G1) /
  (A2, G2_G3_一般) / (B1, *) → 全 / (B2, *) → 全
- `score_motor_run()`:
  - B2 が 1 着 → (125, 1)
  - A1 が一般戦で 4 着 → (20, 1)
  - A2 が SG で 1 着 → (125, 1)
  - PG1 を SG_G1 として扱うこと(A1 + PG1 + 2着 → (80, 1))
  - **転 / 落 / 沈 / エ → (-100, 1)**(4 トークン全て)
  - **F / L / 失 / 妨 → None**(選手起因はノーカウント)
  - 欠 / 不 → None(無効走)
  - 未知トークン "?" → None
- `normalize_token()`: 全角 `４` → `"4"`、全角 `Ｆ` → `"F"`、`nan`/空白 → None
- `detect_session_end_days()`: 日付差 1 日でまとめ、差 2 日以上で分割。`max_sessions=10` 指定で 10 件まで返す
- `load_motor_period_starts()`: motor_stats が当日無くても 14 日遡って取得できること、
  同一場の場合は最新スナップショットだけが採用されること
- `load_motor_history()` 期境界フィルタ: モック period_starts を渡して、
  起算日より前の節が剪定されること(8 節与えて 5 節未満になるケース含む)
- `motor_ability_pt()` 合計確認: 2 節分の MotorRun を渡して期待平均値が返ること
  (うち 1 走を `転` にしたとき分子が -100 寄与することの検算)

### 6.2 統合テスト

- `compute_features_for_day(repo, 既知の日)` を呼び出し、`motor` 列が NaN だらけでなく
  数百モーター分の値が返ること(リグレッション検知)
- 古い日付(履歴不足)では多くが NaN、新しい日付では NaN が大幅に減ることを確認

### 6.3 重み学習回帰テスト

- 旧 motor_pt と新 motor_ability_pt を同月で個別に重み学習し、`r2` 比較表を
  `tmp/motor_ability_r2_compare.csv` に出力する一時スクリプト
  (`scripts/tests/compare_motor_indices.py`)を用意し、リリース前に必ず実行する

---

## 7. ドキュメント更新箇所(CLAUDE.md ルール準拠)

| 更新ファイル | 内容 |
| --- | --- |
| `docs/data/estimate.md` | `N枠_モーターpt` の説明を新ロジック(直近 5 節 × 級別グレード得点平均)に書き換え、補完ルール節も「データなし→NaN」のままで OK だが「勝率→能力指数」へ表記変更 |
| `docs/data/README.md` | 派生データ表は変更なし(列名据え置き)。「`motor_stats` は副次的に勝率特徴量として残存」と注記 |
| 新規 `docs/data/motor_ability_score.md` | スコアテーブル CSV の場所・スキーマ・例 |
| `docs/development.md` | `scripts/build_index.py` の依存関係に title / results/realtime / motor_ability_score.csv を追記 |
| `docs/design/motor_ability_index.md` | 本ファイル |

---

## 8. リリース手順

1. `data/estimate/motor_ability_score.csv` をコミット
2. `scripts/boatrace/index_features.py` 改修(履歴ビルダー追加 + motor_ability_pt 関数 +
   `compute_features_for_day` から呼び出し変更)
3. `scripts/tests/test_motor_ability.py` を追加し pytest が通ることを確認
4. リリース対象月の重みファイル `data/estimate/stadium/index_weights/YYYY-MM.csv` を
   再生成(`python scripts/build_weights.py --month YYYY-MM`)
5. 当日分 index を `python scripts/build_index.py --mode daily --date $TODAY_JST --force`
   で再計算し、`N枠_モーターpt` の分布(平均 50, 標準偏差 ≈ 10)を目視確認
6. `docs/` 一式を同 PR で更新(CLAUDE.md ルール)
7. 旧 `motor_pt` の参照が `compute_features_for_day` 以外に残っていないことを grep で確認

---

## 9. 未決事項 / 将来課題

- **モーター履歴のキャッシュ化**: race_cards スキャンが重くなったら parquet キャッシュ導入
- **節最終日の最終スロット欠損**: race-card scraper を午後に再実行して 14 スロットを
  確実に埋めるか、節翌日の race_cards(別場/別節)に頼らず results/realtime から
  最終日のみ補完するハイブリッド方式を検討
- **「直近 5 節」の N を場別に最適化**: 学習窓を変えると r² が改善する場があるかも
- **マイナス点 -100 の妥当性検証**: 1 着 +125 〜 -100 のレンジ。事故 1 回が 1 着 1 回分
  (B2 級の +125)に拮抗するスケール。実データの分布を見て -50 / -100 / -150 など
  パラメトリックに比較し最適値を選び直す余地あり
- **`F` を選手起因として除外する一貫性**: フライング多発モーター(始動性異常など)が
  存在する場合、選手起因扱いだと取りこぼす。bc_rs1_2 の F 詳細でモーター由来 F を
  抽出できれば再評価したい
- **`motor_stats` 未収録場の扱い**: 期境界フィルタが効かない場(motor_stats 未収録)が
  生じた場合、`scripts/motor-stats.py` の対象場拡張やバックフィル戦略を検討
