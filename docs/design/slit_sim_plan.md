# 確率的スリット(展開モンテカルロ)実装計画

[`slit_tenkai.md`](./slit_tenkai.md) の検証で「スリット隊形は結果を強く規定するが、
点推定では 7 割外す」ことが分かった。その結論を受けて、スリットを **1 本の点推定**
ではなく **ST の予測分布からのサンプリング** で表現し、隊形・決まり手の確率として
出力する仕組みの実装計画。

- **目的**: スタート予想図と展開説明の**誠実化**。「たぶんこの隊形」ではなく
  「ほぼ揃い 57% / イン遅れ 24% / イン先手 19%」と出す
- **非目的**: 回収率の改善。既存 predictor の `component_keys` は**一切触らない**。
  slit_sim は表示専用データであり、`強さpt` にも買い目にも影響させない
  ([`slit_tenkai.md`](./slit_tenkai.md) §4.1 のとおり上積みは 0.018 で、
  独立スロットを立てて A/B する価値がない)
- **対象リポジトリ**: boatracecsv(生成)+ fun-site(表示)

> ## ⚠ Phase 0 の結果(2026-08-10): 本命案は不合格。**撤退ライン 3(ST 帯のみ)で実装する**
>
> 検証記録は [`notebooks/slit_sim/report.md`](../../notebooks/slit_sim/report.md)。
>
> | 案 | 判定 |
> | --- | --- |
> | 隊形 6 分類 + 決まり手(§2 の本命) | **FAIL** — 4 基準すべて。隊形 Brier 0.6402 が定数予測 0.6343 に**負ける** |
> | 撤退 1: 隊形 3 分類 | **FAIL** — σ スケールを全探索しても五分位校正が 7.5pt 未満にならない |
> | 撤退 2: 決まり手を落とす | **FAIL** — 撤退 1 が前提のため同時に不成立 |
> | **撤退 3: ST 帯 (p25〜p75) のみ** | **PASS** — Student-t で被覆率が名目 ±1pt 以内 |
>
> 原因は調整不足ではなく構造的なもの。`in_margin`(1 コースの前後量)の
> **分散のうち事前に説明できるのは 3.9%**(corr 0.197)しかなく、
> レース単位の散らばりも観測変数と相関しない(A1 人数 -0.109 / 風速 0.065 / 場 -0.016)。
> どうサンプリングしても出力はレース非依存の定数に潰れる。
>
> **以下 §1〜§5 は当初計画の記録。実際に実装するスコープは §9 を見ること。**

---

## 0. 事前に判明している設計上の落とし穴

プロトタイプ(2025-11〜2026-08 の 36,767 レース)で以下を確認済み。
**Phase 1 に入る前にこれらを織り込むこと。素朴な実装は必ずミスキャリブレーションする。**

### 0.1 ST 分散の 56% は「レース共通ショック」

| 分解 | sd | 分散比 |
| --- | ---: | ---: |
| 実測 ST(周辺) | 0.0721 | 100% |
| レース平均 ST(共通成分: 風・水面・場全体の緩急) | 0.0539 | 55.9% |
| レース内偏差(個別成分) | 0.0479 | 44.1% |

スリット隊形は **レース内の相対位置**なので、共通成分は完全にキャンセルする。
サンプリングに使う σ は選手の周辺 sd(0.067)ではなく**レース内残差 sd(≈0.047)**。
周辺 sd をそのまま使うと隊形が散らばりすぎる(下表)。

### 0.2 コース別 ST オフセットを入れないと隊形が偏る

`in_margin`(1 コースが外の最先行艇より前に出ている量, m)の分布:

| | mean | sd | 揃い(±0.6m) | 大幅遅れ(≦-1.75m) |
| --- | ---: | ---: | ---: | ---: |
| 実測 | -0.497 | 0.881 | 56.7% | 7.6% |
| MC(全国平均ST + 独立正規 σ=0.0465) | -0.754 | 0.778 | 38.5% | 10.1% |

平均が -0.25m ずれる原因は、全国平均 ST が**コース非依存**なこと。実測のコース別平均 ST は
1C 0.151 / 6C 0.181 と 0.030 秒(0.42m)の系統差がある。
`racer_st.py` の `COURSE_OFFSET`(1C -0.0090 〜 6C +0.0119)はこれを部分的に持つので、
**MC の μ には全国平均 ST ではなく `data/estimate/racer_st/` の推定 ST を使う**こと。

### 0.3 予測偏差には収縮が必要

予測 ST のレース内偏差 `pdev` と実測偏差 `dev` の相関は 0.241、回帰係数 β = 0.586。
`pdev` をそのまま使うと予測が自信過剰になる。μ は
`race_mean + β × (推定ST - race_mean)` として縮める。β は Phase 0 で racer_st 版に対して再推定する。

### 0.4 レース内偏差は極端に裾が厚い(超過尖度 26.1)

| 分位 | 実測 \|dev\| | 同 sd の正規 |
| --- | ---: | ---: |
| 50% | 0.0283 | 0.0328 |
| 90% | 0.0750 | 0.0799 |
| 99% | 0.1450 | 0.1252 |

芯は正規より鋭く、裾ははるかに重い(大出遅れ)。Student-t(df≈5.9, scale≈0.0388)が
よく当てはまる。ただし**プロトタイプでは t 化しても隊形の周辺分布はほとんど改善しなかった**
(0.2 のコースオフセット欠落のほうが支配的)。t の採用可否は Phase 0 で 0.2/0.3 を
直した状態で判定すること。先に t を入れて満足しないこと。

### 0.5 選手別 σ は実在する(作る価値がある)

30 走以上の 1,544 選手を期間で半分に割った split-half:

- sd の前半 vs 後半の相関 **0.555**(平均 ST の相関 0.782 に次ぐ)
- 最安定五分位 sd 0.0576 / 最不安定五分位 sd 0.0762(**32% 差**)

グローバル σ 固定でも動くが、選手別 σ は Phase 1 で入れるだけの再現性がある。

---

## 1. 全体構成

```
boatracecsv (Python)                          fun-site (TS)
─────────────────────────                     ──────────────────────
racer_st.py  ── 推定ST μ + σ                  fetcher/slit-sim-schemas.ts
     │                                              │
slit_sim.py  ── MC サンプリング                site-builder/prediction-builder.ts
     │         隊形分類 → 確率                       │
build_slit_sim.py                             StartPredictionDiagram.astro (ST 帯)
     │                                        TenkaiScenario.astro (新規)
data/estimate/slit_sim/YYYY/MM/DD.csv ───────▶
```

計算は **boatracecsv 側**に置く。理由:

- σ の推定と隊形テーブルの当てはめは実測 ST 履歴に依存し、その履歴と状態機械
  (`racer_st.py` の `state.csv`)が既に Python 側にある
- pytest で数値検証できる(`scripts/tests/`)。fun-site 側は描画に専念させる
- 出力 CSV が他用途(振り返り分析)にも再利用できる

---

## 2. Phase 0 — 校正パラメータの確定(notebook)

**成果物**: `notebooks/slit_sim/calibration.ipynb` + `report.md` + 凍結する定数一式

`racer_st.py` の推定 ST を μ に使い、以下を決める。

| パラメータ | 決め方 |
| --- | --- |
| `BETA`(収縮係数) | 推定 ST のレース内偏差 → 実測偏差 の回帰係数 |
| `SIGMA_GLOBAL` | 収縮後残差のレース内 sd |
| `SIGMA_BY_COURSE` | コース別残差 sd(6C は他より大きい見込み) |
| 選手別 σ の収縮定数 `K_SIGMA` | `racer_st.K_PRIOR`(=10)に倣い split-half で決定 |
| 分布形(正規 / Student-t df) | §0.4 の判定。t なら df と scale |
| 隊形テーブル | [`slit_tenkai.md`](./slit_tenkai.md) §3.1 の 6 分類 × 決まり手 3 値 |

**学習窓 / テスト窓**(決定): 学習 **2026-03-01〜06-20** / テスト **2026-06-21 以降**。
`racer_st.py` の Phase 2 と同一窓に揃える。μ を供給する推定 ST の定数がこの窓で
凍結されている以上、σ と β を別窓で引くと μ の残差構造とずれる。
2025-11 以降の全期間を使わないのはこの理由による(サンプル数より整合性を優先)。

**受け入れ基準**(上記テスト窓、レース単位で時系列分割):

1. `in_margin` の mean / sd / `|·|≦0.6m` 率 が実測と ±0.05m / ±0.05 / ±3pt 以内
2. 隊形 6 分類の周辺確率が実測出現率と **各 ±3pt 以内**
   (プロトタイプの現状: 「ほぼ揃い」39.6% vs 実測 56.9% で **不合格**)
3. 決まり手 3 値の五分位校正で、各ビンの予測平均と実測が ±5pt 以内
   (現状: 逃げ 最上位ビン 予測 52% vs 実測 65% で **不合格**)
4. 隊形確率の Brier score が「全レース一律に周辺分布を出す」定数予測を下回る

> **1〜3 が通らない限り Phase 1 に進まない。** 校正が取れていない確率を UI に出すのは、
> 点推定スリットより悪い(数字がついている分だけ信用されてしまう)。
> どうしても通らない場合の撤退ラインは §6。

**工数目安**: 2〜3 日。

---

## 3. Phase 1 — boatracecsv 側の実装

### 3.1 `scripts/boatrace/racer_st.py` に σ 推定を追加

現状の状態ファイルは `登録番号, 重み付き和, 重み計, 基準日` の 3 変数 EWMA。
σ 用に **二乗和** の列を 1 本足すだけで同じ増分更新が使える。

```
data/estimate/racer_st/state.csv
  登録番号, 重み付き和, 重み付き二乗和, 重み計, 基準日
                ^^^^^^^^^^^^^^ 追加
```

- `RacerStState.add_run()` に `ws2 += st**2` を追加
- `estimate_sigma(regno, prior_sigma)` を新設:
  `var = ws2/wt - (ws/wt)**2` を `K_SIGMA` ぶん `prior_sigma`(コース別 σ)へ収縮
- **互換性**: 既存 `state.csv` に新列が無い。`load_state()` は欠損時 `重み付き二乗和=0`
  として読み、`build_racer_st.py --rebuild` で作り直す運用にする。
  中途半端な状態で σ が過小になるのを避けるため、**リリース時に 1 回 `--rebuild` を回す**
- 出力 CSV に `N枠_推定ST_sd` 列を追加(既存 `N枠_推定ST` の隣)

> 注意: `load_day_runs()` は F(負値 ST)を履歴から除外している。σ でも同じ扱いにする。
> F を含めると σ が跳ね上がり、隊形が散らばりすぎる。

**触るファイル**: `scripts/boatrace/racer_st.py` / `scripts/build_racer_st.py` /
`scripts/tests/unit/test_racer_st.py`

### 3.2 `scripts/boatrace/slit_sim.py`(新規)

```python
SIGMA_BY_COURSE: dict[int, float]   # Phase 0 で凍結
BETA: float
KIMARITE_BY_PATTERN: dict[str, tuple[float, float, float]]  # 逃げ/まくり系/差し

def classify_formation(st: np.ndarray) -> np.ndarray:
    """(..., 6) の ST → 隊形ラベル。slit_tenkai.md §3.1 と同一定義。"""

def simulate(mu: np.ndarray, sigma: np.ndarray, n: int = 2000, seed: int) -> SlitSimResult:
    """μ を BETA で収縮 → n 本サンプル → 隊形確率・先手率・ST 分位点を返す。"""
```

**daily の進入コース(決定)**: **枠なり固定**。`Nコース_艇番 = N` として計算し、
stt が入った時点で realtime 行に差し替える。前検情報から進入を予想する余地はあるが、
枠なり以外の進入は当日朝には根拠が無く、外すと隊形が丸ごとずれる。
daily は「暫定表示」と割り切る(`build_index.py` が preview 由来成分を 50 で
中立化しているのと同じ思想)。

**6 艇立て以外の扱い(決定)**: `build_index.py` に倣い、**列スキーマは常に 6 コース分**を
保ち、不在コースは空欄にする(同ファイルが欠場枠の全列を NaN で埋めるのと同じ)。
その上で:

- `Nコース_推定ST` / `_ST_p25` / `_ST_p75` / `_先手率` は **出す**。
  艇数に依存しない量なので、実在するコースぶんだけ MC を回せばよい
- `隊形_*` と `決まり手_*` は **空欄にする**。隊形テーブルは 6 艇立てで当てはめた
  もので、5 艇では「センター(2-4C)/ 外(5-6C)」の区分自体が変質する。
  校正できない数字を出さない、という Phase 0 の受け入れ基準と同じ判断
- 該当は 442 / 42,228 レース(**1.05%**。5 艇 436・4 艇 6)。特別扱いのコストは小さい

- **乱数は seed 固定**(`レースコード` から導出)。同じ入力で必ず同じ出力にする。
  日次バッチが再実行されるたびに確率が揺れると差分ノイズになり、git 履歴が汚れる
- サンプル数 2000。1 レース 6 艇 × 2000 = 12,000 draw、1 日 ~1,500 レースで
  1,800 万 draw。numpy ベクトル化で数秒。**レースごとの Python ループは書かない**

### 3.3 `scripts/build_slit_sim.py`(新規)+ 出力 CSV

`data/estimate/slit_sim/YYYY/MM/DD.csv`(1 レース 1 行):

| 列 | 内容 |
| --- | --- |
| `レースコード` / `レース日` / `レース場コード` / `レース回` | 他ファイルと共通 |
| `状態` | `daily`(枠なり進入固定)/ `realtime`(stt 進入確定) |
| `Nコース_艇番` | daily は枠番と同一(枠なり固定)、realtime は stt 由来。不在コースは空欄 |
| `Nコース_推定ST` / `Nコース_ST_p25` / `Nコース_ST_p75` | 帯描画用 |
| `Nコース_先手率` | そのコースがスリット先頭になる確率 |
| `隊形_イン明確先手` … `隊形_大幅遅れ外` (6 列) | 合計 1.0 |
| `決まり手_逃げ` / `決まり手_まくり系` / `決まり手_差し` | 隊形確率 × §2 の表 |

> **`Nコース_1着率` は出さない。** スリットだけの 1 着率はモーター・選手の
> 強さを無視した数字で、`強さpt` 由来の買い目と並べると矛盾して見える。
> 展開の言葉(隊形・決まり手)に留める。

CLI は `build_index.py` に合わせる:

```bash
python scripts/build_slit_sim.py --date 2026-08-09 --mode daily
python scripts/build_slit_sim.py --date 2026-08-09 --mode realtime --races 1001,1002
```

### 3.4 バッチへの接続

- **daily**: `infra/run-daily-sync.sh` の `build-racer-st` ステップ直後に
  `build-slit-sim` を追加(推定 ST に依存するため順序必須)。
  sparse_paths に `data/estimate/slit_sim` を追加、`git add` 対象にも追加
- **realtime**: `scripts/preview-realtime.py` が `build_index.update_index_for_races` を
  呼んでいるのと同じ箇所(preview 追記直後)で `update_slit_sim_for_races` を呼ぶ。
  stt の進入コースが入った時点で `状態=realtime` の行に差し替える

### 3.5 ドキュメント(CLAUDE.md のルールにより同 PR 必須)

| ファイル | 追記内容 |
| --- | --- |
| [`docs/data/estimate.md`](../data/estimate.md) | Slit Sim 節を新設(列構成・生成タイミング)。Racer ST 節に `N枠_推定ST_sd` を追記 |
| [`docs/data/README.md`](../data/README.md) | 新ファイル種別として slit_sim を追加 |
| [`docs/development.md`](../development.md) | `build_slit_sim.py` の CLI |
| [`docs/operations.md`](../operations.md) | daily / realtime の実行順序、`--rebuild` の 1 回運用 |
| [`docs/infrastructure.md`](../infrastructure.md) | `run-daily-sync.sh` の sparse-checkout 追加 |

**工数目安**: 3〜4 日。

---

## 4. Phase 2 — fun-site 側の表示

### 4.1 データ取り込み

- `packages/batch/src/fetcher/csv-client.ts` — `"slit_sim"` を `CsvKind` と
  パスマップ(`estimate/slit_sim`)に追加
- `packages/batch/src/fetcher/slit-sim-schemas.ts`(新規)— `racer-st-schemas.ts` と同型
- `packages/batch/src/fetcher/index.ts` — `fetchAndParse("slit_sim", ...)` を追加。
  **未生成日は空配列**でフォールバックすること(racer_st と同じ扱い)

### 4.2 型

`packages/shared/src/types/prediction.ts`:

```ts
/** 確率的スリット。slit_sim CSV 由来。無い日は undefined */
export type SlitSimulation = {
  readonly state: "daily" | "realtime";
  readonly entries: readonly {
    readonly courseNumber: number;
    readonly boatNumber: number;
    readonly st: number;        // 中央値
    readonly stP25: number;
    readonly stP75: number;
    readonly leadProbability: number;   // 先手率
  }[];
  /** 6 艇立て以外は undefined(CSV が空欄。§3.3 の決定) */
  readonly formations?: readonly { readonly key: FormationKey; readonly probability: number }[];
  readonly kimarite?: { readonly nige: number; readonly makuri: number; readonly sashi: number };
};
```

`formations` / `kimarite` を optional にしているのは 6 艇立て以外への対応。
`entries` は常に存在する(不在コースは配列から落とす)。

`RacePrediction` に `readonly slitSimulation?: SlitSimulation;` を追加(optional なので
既存 JSON との後方互換が保てる)。

### 4.3 描画

- **`StartPredictionDiagram.astro`**: `slitSimulation` があれば各レーンに
  p25〜p75 の**半透明バンド**を敷き、中央値位置に現行のマーカーを置く。
  無ければ現状どおり点のみ。`ST_MIN=-0.05 / ST_MAX=0.3` のレンジと `stToX()` は
  そのまま使える。注記を「AI推定ST(帯は 25〜75 パーセンタイル)」に更新
- **`TenkaiScenario.astro`**(新規): 隊形 6 分類の横棒 + 決まり手 3 値。
  `PredictorCard` 内ではなくレースページ直下に置く。**予想者に紐付けない**
  (predictor 非依存のデータなので、予想者タブごとに出すと選択で変わると誤解される)。
  `formations` が undefined(6 艇立て以外)のときは**コンポーネントごと出さない**。
  「データなし」の空枠を置くと欠測を異常に見せてしまう

### 4.4 テスト

- `packages/shared/src/__tests__/` に型ガードと確率合計 1.0 のテスト
- `slit-sim-schemas` のパーサに欠損列 / 空文字のテスト(boatracecsv 側が
  未生成の日に落ちないこと)

**工数目安**: 3〜4 日。

---

## 5. Phase 3 — 運用と継続検証

- **校正モニタ**: `notebooks/slit_sim/` に月次で回す校正チェックを置く。
  Phase 0 の受け入れ基準 1〜3 を実測に対して再評価し、`report.md` に追記
- **再学習トリガ**: 隊形の周辺確率が実測と 5pt 以上乖離した月が 2 ヶ月続いたら
  Phase 0 の定数を引き直す。`racer_st.py` と同じく**定数はコードに凍結**し、
  暗黙に学習し直さない
- **予想者との分離を維持**: `registry.py` の `PredictorSpec` には触らない。
  レビュー時に `component_keys` の差分が入っていたらこの計画から外れている

---

## 6. 撤退ライン(**3 を採用**)

Phase 0 の受け入れ基準 1〜3 が通らない場合、以下の順に縮退する。
**校正が取れないまま Phase 1 に進むことはしない。**
→ 実際に 1・2 とも不成立で **3 を採用**した。改訂後のスコープは §9。

1. **隊形 6 分類 → 3 分類に粗くする**(イン先手 / 揃い / イン遅れ)。
   分類が粗いほど校正は取りやすい
2. **決まり手確率をやめ、隊形確率だけ出す**。決まり手は隊形テーブル経由の
   二段推論なので誤差が乗りやすい
3. **確率をやめ、ST 帯(p25〜p75)の描画だけ実装する**。
   §0 の分散分解が正しければ帯の幅は素直に出る。これだけでも
   「点推定スリットは 7 割外す」問題への回答にはなっており、
   Phase 1 の 3.1(σ 推定)と Phase 2 の 4.3 前半のみで到達できる

3 まで落ちるなら実装規模は約 1/3(2〜3 日)になる。

---

## 7. 見積まとめ

| Phase | 内容 | 目安 | 前提 |
| --- | --- | ---: | --- |
| 0 | 校正パラメータ確定(notebook) | 2〜3 日 | — |
| 1 | boatracecsv: σ 推定 + MC + CSV + バッチ + docs | 3〜4 日 | Phase 0 合格 |
| 2 | fun-site: 取り込み + 型 + 描画 + テスト | 3〜4 日 | Phase 1 の CSV |
| 3 | 校正モニタ整備 | 1 日 | Phase 2 |

Phase 1 と 2 は CSV スキーマ(§3.3)さえ先に固めれば並行できる。

---

## 8. 決定事項

着手前の未決 3 点はいずれも決着済み。実装時に蒸し返さないこと。

| 論点 | 決定 | 記載箇所 |
| --- | --- | --- |
| Phase 0 の学習窓 | `racer_st.py` Phase 2 と同一の **2026-03-01〜06-20**(テストは 06-21 以降)。μ の定数がこの窓で凍結されているため、σ / β を別窓で引くと残差構造がずれる | §2 |
| `状態=daily` の進入コース | **枠なり固定**。stt 取得後に realtime 行へ差し替える | §3.2 |
| 6 艇立て以外(1.05%) | `build_index.py` に倣い **6 コース分の列スキーマを維持**。ST 帯・先手率は出し、`隊形_*` / `決まり手_*` は空欄 | §3.2 / §4.2 |

Phase 0 の結果に依存していた 2 点も決着した。

| 論点 | 決定 |
| --- | --- |
| 分布形 | **Student-t**(df 9.14 / scale 0.879)。正規は被覆が名目より +3.5pt 過大 |
| 隊形 6 分類のまま出せるか | **出さない**。§6 の撤退 3 へ |

---

## 9. 改訂後の実装スコープ(撤退ライン 3)

「ST の予測分布を帯で描く」だけを実装する。**隊形確率・決まり手確率は作らない。**

### 9.1 boatracecsv 側

**`scripts/boatrace/racer_st.py` に σ を追加**(当初計画 §3.1 のまま):

- `state.csv` に `重み付き二乗和` 列を追加。`RacerStState.add_run()` で `ws2 += st**2`
- `estimate_sigma(regno)`: `var = ws2/wt - (ws/wt)**2` を `K_SIGMA=10` で `SD_PRIOR` へ収縮し、
  `SD_PRIOR` で割った**相対倍率**を返す
- 既存 `state.csv` に新列が無いため、リリース時に `build_racer_st.py --rebuild` を 1 回回す
- 出力 CSV に 2 列追加:

| 列 | 内容 |
| --- | --- |
| `N枠_推定ST_p25` | `推定ST + t.ppf(0.25, 9.14) × 0.879 × 0.0684 × 相対倍率` |
| `N枠_推定ST_p75` | 同 `0.75` |

> **`build_slit_sim.py` と `data/estimate/slit_sim/` は作らない。** 出力は既存の
> `racer_st` CSV に 2 列足すだけで足りる。ファイルを増やすと sparse-checkout・
> GCS ミラー・fetcher をすべて増やすことになり、得るものに対して割に合わない。
> 当初計画 §3.2〜§3.4 は破棄。

**進入コースの扱い**: 帯幅は進入コースにほぼ依存しない(σ のコース差は
1C 0.048〜6C 0.053)ので、`racer_st` が持つ**枠番ベースのまま**でよい。
daily / realtime の 2 段構えも不要 — 当初計画 §3.4 の realtime フックは破棄。

**6 艇立て以外**: `racer_st` は枠単位の出力なので特別扱い不要(§8 の決定は
slit_sim CSV 前提だったため失効)。

**ドキュメント**: [`docs/data/estimate.md`](../data/estimate.md) の Racer ST 節に
2 列を追記。新ファイル種別が無いので `docs/data/README.md` /
`docs/infrastructure.md` の変更は不要。`--rebuild` の 1 回運用は
[`docs/operations.md`](../operations.md) に記載する。

### 9.2 fun-site 側

- `packages/batch/src/fetcher/racer-st-schemas.ts` — `estimatedStP25` / `estimatedStP75` を追加
- `packages/shared/src/types/prediction.ts` — `StartPredictionEntry` に
  `readonly startTimingP25?: number` / `startTimingP75?: number` を追加(optional で後方互換)
- `packages/batch/src/site-builder/prediction-builder.ts` — `startTimingFor()` の隣で帯を詰める
- `packages/web/src/components/StartPredictionDiagram.astro` — 各レーンに p25〜p75 の
  半透明バンドを敷く。`stToX()` と表示レンジはそのまま使える。
  注記を「AI推定ST(帯は 25〜75 パーセンタイル)」に更新
- **`TenkaiScenario.astro` は作らない**

**スタート予想図を `useEstimatedST` から切り離す(必須)**: 帯を持つのは AI 推定 ST 版
(`startPredictionEstimated`)だけだが、これを描画するのは `PredictorSpec.useEstimatedST`
が true の予想者のカードに限られていた。その 3 者(v5_slit / v7_aggregate / v8_aionly)は
2026-08-10 までに全て退役したため、そのままでは**帯がどのカードにも出ない**。

- レース詳細ページは予想者に依らず `startPredictionEstimated ?? startPrediction` を渡す
- `oneMarkDistanceOptionsFor()`(1 マーク距離 → 買い目 → 回収率)は**触らない**。
  こちらは従来どおり `useEstimatedST` 駆動で、A/B の対象のまま
- 図は表示専用で回収率に効かないので、予想者ごとに出し分ける理由が無い。
  §9 冒頭の非目的(回収率に影響させない)と整合する

帯幅は中央値 0.082 秒 = 1.14m(≒0.4 艇身)なので、図の上で十分見える。

### 9.3 進捗

| Phase | 内容 | 状態 |
| --- | --- | --- |
| 0 | 校正 | **完了** — [`notebooks/slit_sim/report.md`](../../notebooks/slit_sim/report.md) |
| 1 | boatracecsv: σ 推定 + 2 列追加 + docs | **完了** |
| 2 | fun-site: 取り込み + 型 + 帯描画 | **完了**(実サイトでの目視は未) |
| 3 | 月次の被覆率モニタ | 未着手 |

Phase 1 で入ったもの:

- `racer_st.py` — `state.csv` に `重み付き二乗和` を追加、`RacerStState.sigma_multiplier()` /
  `estimate_band_for_racer()`、定数 `SIGMA_BASE` / `K_SIGMA` / `SD_PRIOR` / `Q75_K`
- 出力 CSV に `N枠_推定ST_p25` / `N枠_推定ST_p75`
- 旧 `state.csv`(二乗和なし)は倍率 1.0 に退避して読める。リリース時に `--rebuild` を 1 回

Phase 2 で入ったもの:

- `RacerStEntry` に `estimatedStP25` / `estimatedStP75`(旧 CSV は null)
- `RaceRacer` に同 2 フィールド、`StartPredictionEntry` に `startTimingP25` / `startTimingP75`
- `startBandFor()` — 推定 ST 版かつ帯が揃う枠にだけ付ける
  (全国平均 ST にフォールバックした枠は帯なし)
- `StartPredictionDiagram.astro` — 艇の背後に艇色 opacity 0.22 の帯 + 凡例
- レース詳細ページ — スタート予想図を `useEstimatedST` から切り離し、
  全予想者で AI 推定 ST 版を渡す(1 マーク距離 = 買い目側は従来どおり)

### 9.4 残作業

- **`build_racer_st.py --rebuild` を本番で 1 回**(未実施。実行するまで帯は全選手同じ幅)
- **実サイトでの目視確認**(未)。web パッケージにローカルデータが無く、
  新列も未公開のため未実施。帯の座標計算は検算済み(トラック 460px 中 111px ≒ 24%)
- Phase 3 の被覆率モニタ

### 9.5 再挑戦の条件

隊形予想は「今のデータでは無理」であって「原理的に無理」ではない。
新しい情報源(周回展示の行き足、部品交換情報、直前気象の細分化など)が
取得できるようになった時点で `notebooks/slit_sim/diagnose.py` の
`corr(実測 in_margin, 予測 in_margin)` を測り直し、
**R² が 0.15 を超えたら** §2 の本命案を再検討する(現在 0.039)。
