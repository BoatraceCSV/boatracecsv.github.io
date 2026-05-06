# Boatrace Data Automation

ボートレースのデータは、独自フォーマットで分散しており、収集と整形に時間がかかります。
そこで、機械学習で利用しやすいように1レース1行のCSVファイルを作成しました。
httpsでダウンロードできるため、Agentからのアクセスにも利用しやすくなっています。
更新は1日1回です。
最新の情報が必要な場合、[Boatrace OpenAPI](https://github.com/BoatraceOpenAPI) などの別のソースをご利用ください。

## データファイル

毎日、以下のCSVファイルが自動生成されます。各ファイルはレースの異なる段階のデータを含みます。

### Programs (出場艇情報)
**ファイルパス**: `data/programs/YYYY/MM/DD.csv`
**URL**: https://boatracecsv.github.io/data/programs/2026/01/01.csv

レース前に公開される出場艇の情報です。選手のプロフィールと成績データを含みます。

#### サンプルデータ（1行目）
```
201601150001,１Ｒ  一般          Ｈ１８００ｍ  電話投票締切予定０８：５５,第5日,2016-01-15,ボートレース唐津,1R,一般,1800,08:55,1,3156,金子良昭,51,静岡,54.0,A1,6.56,52.46,7.68,64.29,26,23.71,51,32.22,1,6,4,5,2,,9,,,,,,,2,3825,宮嵜隆太,42,福岡,60.0,B1,4.29,22.22,4.65,23.08,63,29.46,80,34.57,6,,,,,,5,5,1,5,5,,7,3,...
```

#### 列の詳細説明

**基本情報**（レースの識別情報）:
- `レースコード` (201601150001): レースの一意識別子。YYYYMMDD＋レース場＋レース回の形式
- `タイトル`: レースの名前と開催日程情報（例: 「１Ｒ  一般  Ｈ１８００ｍ  電話投票締切予定０８：５５」）
- `日次` (第5日): 開催期間中の何日目か
- `レース日` (2016-01-15): 開催日付
- `レース場` (ボートレース唐津): 開催地
- `レース回` (1R): 当日の何レース目か
- `レース名` (一般): レースのグレード（一般、女子戦など）
- `距離(m)` (1800): 走行距離（通常1800m）
- `電話投票締切予定` (08:55): 投票受付終了時刻

**各艇の選手情報**（1枠～6枠、計6艇分。以下は1枠の例）:
- `艇番` (1): 艇の識別番号
- `登録番号` (3156): 選手の全国統一登録番号
- `選手名` (金子良昭): 選手の氏名
- `年齢` (51): 選手の年齢
- `支部` (静岡): 選手の所属支部
- `体重` (54.0): 選手の体重（kg）
- `級別` (A1): 選手のランク。A1が最上位、以下A2、B1、B2

**選手の全国成績**（全国での平均的な成績）:
- `全国勝率` (6.56): 全国での1着率（%）
- `全国2連対率` (52.46): 全国での1～2着率（%）

**当地成績**（そのレース場での成績）:
- `当地勝率` (7.68): 当該レース場での1着率（%）
- `当地2連対率` (64.29): 当該レース場での1～2着率（%）

**装備情報**（モーターとボート）:
- `モーター番号` (26): 使用するモーターの番号
- `モーター2連対率` (23.71): そのモーターでの平均1～2着率（%）
- `ボート番号` (51): 使用するボートの番号
- `ボート2連対率` (32.22): そのボートでの平均1～2着率（%）

**当節成績**（その開催期間内での成績。最大6レース分）:
- `今節成績_1-1`, `今節成績_1-2`, ..., `今節成績_6-2`:
  - 形式は「Nレース_M着」（例: `1-1`は1レース目の1着、`2-2`は2レース目の2着）
  - 複数のレースの成績を記録（最大6レース分）
  - 空白は未出走またはレース不開催

**予想情報**:
- `早見` (9): 当日2レース出場する場合、レース番号が記載。1レースのみの場合は空。

> **用途**: 選手の実力評価、地元での成績比較、装備の相性分析、統計的なレース予想に利用

---

### Previews (展示会情報)
**ファイルパス**: `data/previews/YYYY/MM/DD.csv`
**URL**: https://boatracecsv.github.io/data/previews/2026/01/01.csv

展示会での各艇の走行データです。レース当日の朝に実施される展示会での情報を含みます。

**データソース**: 2026年4月以降、`race.boatcast.jp` が配信する TSV (`bc_j_tkz` / `bc_j_stt` / `bc_rs1_2`) を組み合わせて生成しています（それ以前は `www.boatrace.jp` の HTML をスクレイプ）。これに伴い気象6項目 (`風速(m)` / `風向` / `波の高さ(cm)` / `天候` / `気温(℃)` / `水温(℃)`) は、従来の「展示会終了時点」の値ではなく **「当該レースの確定時点 (`bc_rs1_2`) の値」** を記録します。当日内で風速・風向・気温が時間経過で変化するため、過去データと数値が ±数単位ずれる場合があります（艇別の体重・展示タイム・チルト・スタート展示の値は影響を受けません）。

#### サンプルデータ（1行目）
```
201601150301,第３９回日刊スポーツ杯,2016-01-15,3,01R,3.0,1,5.0,1,6.0,11.0,1,1,52.7,0.0,6.8,0.0,-0.13,2,2,51.2,0.0,6.68,0.0,-0.04,3,3,54.8,0.0,6.73,0.5,0.07,4,4,52.1,0.0,6.71,0.0,0.07,5,5,51.0,0.0,6.68,0.5,0.03,6,6,58.9,0.0,6.73,0.0,0.02
```

#### 列の詳細説明

**基本情報**（レースの識別情報）:
- `レースコード` (201601150301): Programs と同じ形式のレース識別子
- `タイトル` (第３９回日刊スポーツ杯): レースのタイトル
- `レース日` (2016-01-15): 開催日付
- `レース場` (3): レース場コード（数字で表記）
- `レース回` (01R): 当日の何レース目か

**環境・気象情報**（レース確定時点のコンディション）:
- `風速(m)` (3.0): 風速（m/s）
- `風向` (1): 風向（コード値。1=北, 2=北東, 3=東, 4=南東, 5=南, 6=南西, 7=西, 8=北西）
- `波の高さ(cm)` (5.0): 波の高さ（cm）
- `天候` (1): 天気（コード値）。1=晴, 2=曇, 3=雨。2026-04 以降のデータでは 4=雪, 5=台風, 6=霧, 9=その他 を新たに区別します。それ以前 (HTML 由来) のデータでは 4=大雨, 5=霧 を表していたためコード解釈が異なる点に注意。
- `気温(℃)` (6.0): 気温（℃）
- `水温(℃)` (11.0): 水温（℃）

**各艇の展示会データ**（1艇～6艇。以下は1艇の例）:
- `艇番` (1): 艇の識別番号
- `コース` (1): 進入予定コース（1～6）
- `体重(kg)` (52.7): 選手の体重（kg）
- `体重調整(kg)` (0.0): ハンデ調整による追加体重（kg）
- `展示タイム` (6.8): 展示会での走行タイム（秒）
- `チルト調整` (0.0): エンジンの傾け角度調整値
- `スタート展示` (-0.13): スタート際での速度計測値（秒単位の誤差）

> **用途**: レース当日のコンディション把握、艇の調整状況確認、展示会での走行速度分析、予想の参考情報

---

### Realtime Preview (締切5分前の直前情報)

`scripts/preview-realtime.py` が GitHub Actions で **JST 08:30〜23:00 の毎分** 動作し、各レースの締切5分前のスナップショットを **データソース単位で別ファイル** に追記します。`scripts/preview.py`（1日1回バッチ）と異なり、当日の値の時間変化を保持できます（特に水面気象）。

**ファイルパス**:
- `data/previews/tkz/YYYY/MM/DD.csv` — 体重・展示タイム・チルト
- `data/previews/stt/YYYY/MM/DD.csv` — 進入コース・スタート展示
- `data/previews/sui/YYYY/MM/DD.csv` — 水面気象スナップショット
- `data/previews/original_exhibition/YYYY/MM/DD.csv` — オリジナル展示データ

**URL 例**: https://boatracecsv.github.io/data/previews/tkz/2026/05/03.csv

**共通カラム（4ファイル先頭6列）**:
- `レースコード` (`YYYYMMDDjjrr`): Programs / Previews 等と JOIN 可能な12桁識別子
- `レース日` (`YYYY-MM-DD`)
- `レース場` (`01`〜`24`、2桁ゼロ詰め)
- `レース回` (`01R`〜`12R`)
- `締切時刻` (`HH:MM`、`getHoldingList2` 由来)
- `取得日時` (ISO8601, JST、例 `2026-05-03T20:25:03+09:00`)

**ソース固有カラム**:

`tkz` — `状態` (常に `1`) + 6艇 × {`体重(kg)`, `体重調整(kg)`, `展示タイム`, `チルト`}。
`stt` — 6艇 × {`コース`, `スタート展示`}。スタート展示は既存 Previews と同じセマンティクス（F は負値、L は空）。
`sui` — `気象観測時刻` (HHMM) + `風速(m)` / `風向` / `波の高さ(cm)` / `天候` / `気温(℃)` / `水温(℃)`。
`original_exhibition` — `計測数` / `計測項目1` / `計測項目2` / `計測項目3` + 6艇 × {`選手名`, `値1`, `値2`, `値3`}。場ごとに項目が異なります（多くは「一周／まわり足／直線」、住之江・尼崎・徳山は2項目、桐生は「半周ラップ／まわり足／直線」）。

**取得・スキップルール**:
- 中止 / 順延 / 途中中止のレースはスキップ
- ソースファイルがまだ公開されていない、または計測中 (`status=0`) / 計測不能 (`status=2`) の場合は **追記せずスキップ**（次回実行で自動再試行）
- `original_exhibition` は `status=1` のみ追記（旧 `data/original_exhibition/` にあった `ステータス` カラムは廃止）
- 同一 `レースコード` は1日1行のみ（per-source dedup）
- スケジュール実行のため、各レースについて取得は1回のみ

> **用途**: 締切直前のコンディション把握、時系列での風・水温の変化分析、リアルタイム予想モデル特徴量。`レースコード` で他の CSV と JOIN 可能

---

### Results (レース結果)
**ファイルパス**: `data/results/YYYY/MM/DD.csv`
**URL**: https://boatracecsv.github.io/data/results/2026/01/01.csv

レース終了後に公開されるレース結果です。順位、払戻金、詳細な走行情報を含みます。

#### サンプルデータ（1行目）
```
201601152301,ウインターモーニングバトル,第5日,2016/01/15,唐津,01R,一般,1800,晴,南西,2,2,逃げ,1,190,1,110,2,100,1-2,390,2,1-2,330,2,1-2,130,2,1-3,130,1,2-3,150,3,1-2-3,780,3,1-2-3,250,1,1,1,3156,金 子 良 昭,26,51,6.67,1,0.09,108.2,2,2,3825,宮 嵜 隆太郎,63,80,6.73,2,0.15,111.4,3,3,2538,高 橋 二 朗,17,64,6.69,3,0.12,112.7,4,5,3889,須 藤 隆 雄,11,60,6.74,5,0.11,113.0,5,4,3609,泉 祥 史,35,45,6.78,4,0.12,114.0,6,6,4899,占 部 一 真,19,78,6.7,6,0.09,115.9
```

#### 列の詳細説明

**基本情報**（レースの識別情報）:
- `レースコード` (201601152301): Programs/Previews と同じ形式のレース識別子
- `タイトル` (ウインターモーニングバトル): レースのタイトル
- `日次` (第5日): 開催期間中の何日目か
- `レース日` (2016/01/15): 開催日付
- `レース場` (唐津): 開催地
- `レース回` (01R): 当日の何レース目か
- `レース名` (一般): レースのグレード
- `距離(m)` (1800): 走行距離

**当日気象情報**:
- `天候` (晴): 当日の天気
- `風向` (南西): 風の向き
- `風速(m)` (2): 風速（m/s）
- `波の高さ(cm)` (2): 波の高さ（cm）

**決着情報**:
- `決まり手` (逃げ): レース結果の決着パターン（逃げ、差し、まくり等）

**投票・払戻金情報**:
- `単勝_艇番` (1) / `単勝_払戻金` (190): 1着になった艇番と単勝の払戻金
- `複勝_1着_艇番` (1) / `複勝_1着_払戻金` (110): 複勝に入った各着数の艇番と払戻金
- `2連単_組番` (1-2) / `2連単_払戻金` (390): 2連単の組み合わせと払戻金
- `2連単_人気` (2): 2連単の人気度（1位が最高）
- `2連複_組番` (1-2) / `2連複_払戻金` (330) / `2連複_人気` (2)
- `拡連複_1-2着_組番` (1-2) / `拡連複_1-2着_払戻金` (130) / `拡連複_1-2着_人気` (2)
- `拡連複_1-3着_組番` (1-3) / `拡連複_1-3着_払戻金` (130) / `拡連複_1-3着_人気` (1)
- `拡連複_2-3着_組番` (2-3) / `拡連複_2-3着_払戻金` (150) / `拡連複_2-3着_人気` (3)
- `3連単_組番` (1-2-3) / `3連単_払戻金` (780) / `3連単_人気` (3)
- `3連複_組番` (1-2-3) / `3連複_払戻金` (250) / `3連複_人気` (2)

**各着順の詳細情報**（1着～6着。以下は1着の例）:
- `1着_着順` (1): 着順番号（常に1）
- `1着_艇番` (1): 入着した艇番
- `1着_登録番号` (3156): 選手の全国統一登録番号
- `1着_選手名` (金 子 良 昭): 選手の氏名
- `1着_モーター番号` (26): 使用したモーターの番号
- `1着_ボート番号` (51): 使用したボートの番号
- `1着_展示タイム` (6.67): 展示会での走行タイム
- `1着_進入コース` (1): 実際の進入コース（1～6）
- `1着_スタートタイミング` (0.09): スタート際での速度計測値
- `1着_レースタイム` (108.2): 実際のレース走行時間（秒）

> **用途**: レース結果の統計分析、投票情報の記録、決着パターンの研究、選手やモーター・ボートの勝敗分析

---

### Race Cards (出走表詳細)
**ファイルパス**: `data/programs/race_cards/YYYY/MM/DD.csv`
**URL**: https://boatracecsv.github.io/data/programs/race_cards/2026/04/25.csv

`race.boatcast.jp` の `bc_j_str3` を起源とする出走表詳細データです。Programs と並行して提供される **追加情報** で、Programs にない項目（全国/当地3連対率、全国平均ST、賞除フラグ、F/L本数、モーター/ボート3連対率、節間14スロット成績）を含みます。学習特徴量の拡張として利用してください。Programs CSV はそのまま維持されます。

#### サンプルデータ（1行目、抜粋）
```
レースコード,レース日,レース場コード,レース回,
艇1_登録番号,艇1_選手名,艇1_期別,艇1_支部,艇1_出身地,艇1_年齢,艇1_級別,
艇1_賞除,艇1_F本数,艇1_L本数,
艇1_全国平均ST,艇1_全国勝率,艇1_全国2連対率,艇1_全国3連対率,
艇1_当地勝率,艇1_当地2連対率,艇1_当地3連対率,
艇1_モーターフラグ,艇1_モーター番号,艇1_モーター2連対率,艇1_モーター3連対率,
艇1_ボートフラグ,艇1_ボート番号,艇1_ボート2連対率,艇1_ボート3連対率,
艇1_早見,
艇1_節D1走1_R番号,艇1_節D1走1_進入,艇1_節D1走1_枠,艇1_節D1走1_ST,艇1_節D1走1_着順,
... (節D1走2 〜 節D7走2 まで14スロット × 5項目)
艇2_… (同形式) ...
... 艇6まで ...
202604251712,2026-04-25,17,12R,
3941,池田 浩二,81期,愛知,愛知,48,A1,
,0,0,
0.13,7.88,53.2,71.9,
0.0,0.0,0.0,
0,57,42.9,56.2,
0,35,34.2,52.1,
,
,,,,,12,1,1,0.1,1,5,6,6,0.07,4,11,3,3,0.2,1,...
```

#### 列の詳細説明

**基本情報**:
- `レースコード` (202604251712): Programs/Previews/Results と同じ12桁形式の識別子
- `レース日` (2026-04-25): 開催日付
- `レース場コード` (17): 場コード (1〜24)
- `レース回` (12R)

**艇N 基本プロフィール** (N=1..6, 各艇26列):
- `艇N_登録番号`: 全国統一登録番号
- `艇N_選手名`: 選手名（全角スペースは半角1個に正規化）
- `艇N_期別`: 養成所期別（例 "81期"）
- `艇N_支部` / `艇N_出身地`: 支部および出身地（`bc_j_str3` の `支部:出身地` を分割）
- `艇N_年齢` / `艇N_級別`
- `艇N_賞除`: 賞金除外フラグ（補欠出走など）。該当時 `"賞除"`、なければ空。出現率 ~0.8%
- `艇N_F本数` / `艇N_L本数`: フライング・出遅れの累積本数（0〜N の整数。空白は0扱い）

**全国/当地成績**（Programs より粒度が高い）:
- `艇N_全国平均ST`: 全国期別の平均スタートタイミング
- `艇N_全国勝率` / `艇N_全国2連対率` / `艇N_全国3連対率`: 過去6ヶ月（今節除く）
- `艇N_当地勝率` / `艇N_当地2連対率` / `艇N_当地3連対率`: 過去3年（今節除く）

**モーター/ボート**:
- `艇N_モーターフラグ` / `艇N_ボートフラグ`: 特殊状態フラグ（"1"=該当）
- `艇N_モーター番号` / `艇N_モーター2連対率` / `艇N_モーター3連対率`: 使用開始から前節終了時点
- `艇N_ボート番号` / `艇N_ボート2連対率` / `艇N_ボート3連対率`: 同上

**早見**:
- `艇N_早見`: 当日2レース出場時の他R番号（整数、`bc_j_str3` の `"5R"` 表記から `R` を除去）、なければ空

**節間14スロット成績** (各艇 14 × 5 = 70列):
- スロット順: 1日目1走 → 1日目2走 → 2日目1走 → … → 7日目2走（`bc_j_str3` の col[25]..col[38] に対応）
- 各スロット5項目:
  - `艇N_節D{D}走{S}_R番号`: そのスロットで出走したレース番号
  - `艇N_節D{D}走{S}_進入`: 実際の進入コース (1-6)
  - `艇N_節D{D}走{S}_枠`: 枠番 (1-6)
  - `艇N_節D{D}走{S}_ST`: スタートタイミング（負値はフライング扱い）
  - `艇N_節D{D}走{S}_着順`: 着順または特殊トークン。半角1〜6 / `F` (フライング) / `L` (出遅れ) / `欠` (欠場) / `転` (転覆) / `妨` (妨害失格) / `落` (落水) / `エ` (エンスト) / `不` (不完走)。ソースの全角 `Ｆ` `Ｌ` は半角に正規化済み
- 未出走スロットは全列空欄

**データソース**: `https://race.boatcast.jp/hp_txt/{jo}/bc_j_str3_{ymd}_{jo}_{race}.txt`。利用可能なのは概ね **2025-05-02 以降**。それ以前は Programs を参照してください。

> **用途**: ML特徴量の強化（コース別なし／節間R番号・進入・枠・ST・着順の構造化／3連対率・全国平均ST）。Programs と `レースコード` で JOIN して利用します。

---

### Recent National Form (全国近況5節)
**ファイルパス**: `data/programs/recent_national/YYYY/MM/DD.csv`
**URL**: https://boatracecsv.github.io/data/programs/recent_national/2026/04/25.csv

`race.boatcast.jp` の `bc_zensou` を起源とする、各艇の全国近況5節（直近の節間成績）を集約したファイルです。Programs にない「節グレード」「節期間（開始日〜終了日）」「着順時系列の生文字列」を含み、ML特徴量として強力です。

#### サンプルデータ（1行目、抜粋）
```
レースコード,レース日,レース場コード,レース回,
艇1_登録番号,艇1_選手名,
艇1_前1節_開始日,艇1_前1節_終了日,艇1_前1節_場コード,艇1_前1節_場名,艇1_前1節_グレード,艇1_前1節_着順列,
艇1_前2節_… (同形式) …
… 前5節まで5ブロック ×（艇2〜艇6 同形式）…
202604251712,2026-04-25,17,12R,
3941,池田 浩二,
2026-04-12,2026-04-17,01,桐生,ＧⅠ,６４１　２１６　４　５６,
…
```

#### 列の詳細説明

**基本情報**:
- `レースコード` (202604251712)
- `レース日` (2026-04-25)
- `レース場コード` (17)
- `レース回` (12R)

**艇N 識別** (N=1..6):
- `艇N_登録番号`: 全国統一登録番号
- `艇N_選手名`: 選手名（全角スペース正規化済）

**艇N 前K節成績** (K=1..5、K=1が最新):
- `艇N_前K節_開始日` / `艇N_前K節_終了日`: 節期間（YYYY-MM-DD）
- `艇N_前K節_場コード`: 場コード ("01"-"24")
- `艇N_前K節_場名`: 場名（全角スペース除去済、例 "鳴門"）
- `艇N_前K節_グレード`: "一般" / "ＧⅢ" / "ＧⅡ" / "ＧⅠ" / "ＳＧ" / "ＰＧ１" など
- `艇N_前K節_着順列`: 着順時系列の生文字列。トークン定義:
  - `１`-`６` (全角): 着順 — ソースが全角のため、ML特徴量化時は半角への変換を検討
  - `F`: フライング, `L`: 出遅れ — ソースは全角 `Ｆ` `Ｌ` だが、CSV出力時に半角へ正規化
  - `欠`: 欠場, `転`: 転覆, `妨`: 妨害失格, `落`: 落水
  - `エ`: エンスト, `不`: 不完走, `沈`: 沈没, `失`: 失格（`妨`以外の失格）
  - `[N]`: 優勝戦の着順 N (例 `[１]` = 優勝, `[４]` = 4着)
  - 全角スペース (`　`): 日区切り（同一節内の日違い）

**データソース**: `https://race.boatcast.jp/hp_txt/{jo}/bc_zensou_{ymd}_{jo}.txt`。場×日×1ファイル（全選手の縦持ち）から、B-fileの艇番↔登録番号で逆引きして横持ちCSVに変換。新人選手で5節未満の場合は末尾セッションが空になります。

> **用途**: 直近の調子・節グレード推移・休場頻度を時系列特徴量化。`艇N_前K節_着順列` を文字単位で集計すれば「直近X日の連対率」「F発生数」などを派生できます。

---

### Recent Local Form (当地近況5節)
**ファイルパス**: `data/programs/recent_local/YYYY/MM/DD.csv`
**URL**: https://boatracecsv.github.io/data/programs/recent_local/2026/04/25.csv

Recent National Form と**同一スキーマ**で、ソースだけ `bc_zensou_touchi` に差し替わったファイルです（当該レース場での直近5節のみが収録される）。当地適性をML特徴量化するために利用してください。

**データソース**: `https://race.boatcast.jp/hp_txt/{jo}/bc_zensou_touchi_{ymd}_{jo}.txt`。スキーマ・列名は Recent National Form と完全一致しているため、両ファイルを `(レースコード, 艇N_*)` で同列名の prefix 違いとして取り扱えます（実装時は CSV ファイル分離のためそのまま JOIN 不可、必要なら pandas で suffix 付与）。

> **用途**: 当地で過去どのような戦績だったか、当地特化の特徴量に。

---

### Motor Stats (モーター期成績)
**ファイルパス**: `data/programs/motor_stats/YYYY/MM/DD.csv`
**URL**: https://boatracecsv.github.io/data/programs/motor_stats/2026/04/26.csv

`race.boatcast.jp` の `bc_mst` (モーター期起算日) ＋ `bc_mdc` (期内モーター詳細) を起源とする、**1モーター1行**のスナップショットファイルです。

> **収録範囲**: 1日あたりの収録は **当日開催のある場のみ**（B-fileから抽出）。24場のうち通常 14〜16 場 (~62%) しか含まれません。休場日のある場は当日のレコードが欠けるため、ML 時系列で利用する際は前回スナップショットからのフォワードフィルを推奨します。

> **重要な注意**: race.boatcast.jp は**現在のモーター期のみ**を返却するため、**過去日のバックフィル不可**。`記録日` 以降の日次スナップショットを継続蓄積する設計です。日次バッチ運用が始まった日から有効データが溜まり始めます。

#### サンプルデータ（1行目、抜粋）
```
記録日,モーター期起算日,場コード,モーター番号,
勝率,勝率順位,2連対率,2連対率順位,3連対率,3連対率順位,
1着回数,1着順位,2着回数,2着順位,3着回数,3着順位,
連対外回数,出走数,
優勝回数,優勝順位,優出回数,優出順位,
raw_col_21,raw_col_22,
平均ラップ秒,平均ラップ順位,期内初使用日,
整備種別1回数,整備種別2回数,整備種別3回数,整備種別4回数,整備種別5回数,整備種別6回数,
直近メンテ日
2026-04-25,2025-10-19,17,25,
8.1,1,80.0,1,90.0,1,
5,1,3,4,1,26,
1,10,
1,1,1,1,
677,8,
14.89,1,2025-10-22,
0,3,2,0,0,0,
2025-10-24
```

#### 列の詳細説明（確度ラベル付き）

確度ラベル: ★★★ = JS実装またはデータ分布で確証あり／★★ = 強い状況証拠あり／★ = 仮説段階。

**メタ情報**:
- ★★★ `記録日`: スナップショット取得日（= スクリプトの `--date` 引数、通常は前日JST）
- ★★★ `モーター期起算日`: 当該モーター期の開始日 (`bc_mst` の値)
- ★★★ `場コード`: "01"-"24"
- ★★★ `モーター番号`: 物理モーター番号

**勝率系**（rate × 100 を /100 に変換し小数点表記、順位は1位が最高）:
- ★★★ `勝率` / `勝率順位`
- ★★★ `2連対率` (%) / `2連対率順位`
- ★★★ `3連対率` (%) / `3連対率順位`

**着順回数**（順位は最大値が1位）:
- ★★★ `1着回数` / `1着順位`
- ★★★ `2着回数` / `2着順位`
- ★★★ `3着回数` / `3着順位`
- ★★★ `連対外回数`: 4着以下＋DNF（F・L・欠・転・落・妨など）の合計
- ★★★ `出走数`: 当該モーター期の総出走回数。`1着+2着+3着+連対外 == 出走数` が全データで成立

**Raw 列（意味未確定）**:
- ★ `raw_col_21` / `raw_col_22`: 6.7-7.1 帯の不明指標 + 順位。**場ごとに分布が異なる**（場06=低め群 vs 場18=高め群、レンジ27以上）ため ML 特徴量化時は場ごとの正規化を推奨。要検証

**優勝・優出**（boatcast の MotorHistory.js で確認済み）:
- ★★★ `優勝回数` / `優勝順位`
- ★★★ `優出回数` / `優出順位`

**ラップタイム**:
- ★★ `平均ラップ秒` (×100 を /100): 14.85前後が宮島の典型値。場ごとに分布が異なる（場06=15.00 / 場18=15.20 など）
- ★★ `平均ラップ順位`: 値が小さい (=ラップが短い) ほど上位
- **空欄になる条件**: `1着+2着+3着 == 0`（連対実績ゼロ）のモーターは boatcast 側でラップタイムが算出されないため、`平均ラップ秒` / `平均ラップ順位` / `期内初使用日` の3列が連動して空になる（直近サンプルで 3.0% のモーターに該当）

**日付・整備**:
- ★★ `期内初使用日`: 当該モーターがその期に初出走した日（連対実績ゼロのモーターでは空）
- ★★ `整備種別1回数` 〜 `整備種別6回数`: 6カテゴリ（典型: ピストン/リング/シリンダー/ロアー/キャブレター/その他）の整備実施回数。**カテゴリ番号と項目名のマッピングは未確認**
- ★★ `直近メンテ日`: 直近メンテ実施日（または次回予定日の可能性。要確認）

**データソース**:
- `https://race.boatcast.jp/hp_txt/{jo}/bc_mst_{jo}.txt` — モーター期起算日
- `https://race.boatcast.jp/hp_txt/{jo}/bc_mdc_{period_yyyymmdd}_{jo}.txt` — 期内モーター詳細

> **用途**: Programs の `モーター2連対率` の上位互換。3連対率・優勝回数・整備履歴・平均ラップまで取れるため、モーター強さの強力な特徴量に。日次更新で時系列追跡可能。`(記録日, 場コード, モーター番号)` を主キーとし、`programs` の `艇N_モーター番号` と JOIN して使用。

---

### Strength Index (強さポイント)
**ファイルパス**: `data/estimate/index/YYYY/MM/DD.csv`
**URL**: https://boatracecsv.github.io/data/estimate/index/2026/05/03.csv

各レース 1 行で、6 枠分の「強さポイント」を 5 要素の偏差値で表現したファイルです。**枠番**・**選手**・**モーター**・**展示**・**気象** の 5 要素を場別に学習した重みで線形結合し、平均 50・標準偏差 10 の偏差値スケールで出力します。

**生成パイプライン**:

1. **日次バッチ** (`scripts/build_index.py --mode daily`、JST 00:10): 当日のレース全件について、変動が小さい 3 要素 (枠番・選手・モーター) を計算し、展示・気象は 50 (平均) で補完。状態 = `daily`、暫定の強さpt が入る。
2. **直前バッチ** (`scripts/preview-realtime.py` から内部呼び出し): 各レースの締切 5 分前に preview を取得した直後、対応する index 行の展示・気象を実値で再計算。状態 = `realtime`、強さpt が確定値に更新される。
3. **月次重み学習** (`scripts/build_weights.py --month YYYY-MM`、毎月 1 日 09:00 JST): 直近 6 ヶ月のデータから 24 場 × 5 要素の重みを学習し、`data/estimate/stadium/index_weights/YYYY-MM.csv` を生成。

#### サンプルデータ(1行目、抜粋)

```
レースコード,レース日,レース場コード,レース回,状態,
1枠_枠番pt,1枠_寄与_枠番pt,1枠_選手pt,1枠_寄与_選手pt,1枠_モーターpt,1枠_寄与_モーターpt,1枠_展示pt,1枠_寄与_展示pt,1枠_気象pt,1枠_寄与_気象pt,1枠_強さpt,
2枠_… (同形式 11 列) … 6枠 まで,
202605030101,2026-05-03,01,1R,realtime,
68.84,30.28,36.59,8.06,50.00,5.94,54.93,3.24,18.63,1.30,51.84,
…
```

#### 列の詳細説明

**基本情報**:
- `レースコード` / `レース日` / `レース場コード` / `レース回`: 他ファイルと同じ識別子
- `状態`: `daily`(日次バッチ完了、展示・気象は暫定50)/ `realtime`(直前バッチで展示・気象を実値に更新済み)

**艇 N の 11 列**(N=1..6, 計 66 列):
- `N枠_枠番pt`: 偏差値スケールの 枠番強度。`data/estimate/stadium/win_rate.csv` の場×季節×コース勝率を場別 (μ, σ) で標準化
- `N枠_選手pt`: 偏差値スケールの 選手能力指数。`data/programs/recent_national/` + `data/programs/recent_local/` の着順列をグレード別に得点化(算出基準点合計÷出走回数)し場別標準化。式は br-racers.jp の能力指数算出式に準拠
- `N枠_モーターpt`: 偏差値スケールの モーター強度。`data/programs/motor_stats/` の勝率を場別標準化(勝率0=データなしの場合は欠損として50で補完)
- `N枠_展示pt`: 偏差値スケールの 展示パフォーマンス。展示タイム + オリジナル展示の3項目をレース内偏差値化して平均、その後場別標準化
- `N枠_気象pt`: 偏差値スケールの 気象有利度。`data/estimate/stadium/sui_params.csv` で当日気象から各コースの有利pt変動を計算し場別標準化(コース固定有利は枠番ptに集約済み)
- `N枠_寄与_{要素}pt`: その要素の重み × 偏差値pt(= 強さptへの寄与の内訳)
- `N枠_強さpt`: 5 つの寄与の合計。Σ重み = 1 のため平均 50 ± 10 のスケールに収まる

#### 補完ルール

- 元データが欠損した要素の偏差値ptは **50 で補完**(平均扱い)
- 5 要素のうち 1 つでも欠損があっても 強さpt は計算される
- 重みファイル(`data/estimate/stadium/index_weights/YYYY-MM.csv`)が見つからない月のデータは、すべて NaN を出力

> **用途**: 単発レースの予想に直接使えるランキング指標。`強さpt` 順で買い目を組み立てたり、寄与列でなぜ強い/弱いかを分解できる。重みは 6 ヶ月ローリングで学習されるため、季節変動を反映。

---

### Stadium Parameters (場別パラメータ)

`data/estimate/stadium/` 配下に、index 計算で参照する場別の係数・統計量を保存しています。

#### `data/estimate/stadium/win_rate.csv`

場 × 季節 × コース別の長期勝率テーブル。`枠番pt` の生値ソース。

| 列 | 説明 |
| --- | --- |
| `場コード` | "01"〜"24" |
| `季節` | 春(3-5月)/ 夏(6-8月)/ 秋(9-11月)/ 冬(12-2月) |
| `1コース勝率` 〜 `6コース勝率` | コース別の長期1着率(%) |

#### `data/estimate/stadium/sui_params.csv`

24 場分の気象線形回帰パラメータ。波・風(追い/向かい)・気温水温差・天候から各コースの有利pt変動を計算する係数。1 場 1 行、43 列(stadium + 切片6 + 6特徴量×6コース = 36)。

| 列グループ | 内容 |
| --- | --- |
| `base_c1` 〜 `base_c6` | 基準条件(凪・無風・晴・気温=水温)下の有利pt切片 ※index計算では切片は使わず変動分のみを `気象pt` に反映 |
| `wave_cm_c1〜c6` | 波高 1cm あたりの有利pt変化 |
| `temp_diff_c1〜c6` | 気温-水温差 1℃ あたりの有利pt変化 |
| `wind_tail_ms_c1〜c6` | 追い風 1m/s あたりの有利pt変化 |
| `wind_head_ms_c1〜c6` | 向かい風 1m/s あたりの有利pt変化 |
| `is_cloudy_c1〜c6` | 曇り(vs 晴)による有利ptシフト |
| `is_rainy_c1〜c6` | 雨(vs 晴)による有利ptシフト |

風向は各場の `facing_deg`(スタンド方位)で正規化して 追い風/向かい風/横風 のカテゴリに分けます。場ごとの learned R² は概ね 0.05〜0.20。`scripts/build_sui_params.py` で実データから再学習可能。

#### `data/estimate/stadium/index_weights/YYYY-MM.csv`

毎月 1 日に再学習される 24 場 × 5 要素の重みファイル。学習窓は対象月の 6 ヶ月前〜前月末。各場 1 行、`stadium`, `n_samples`, 5 要素の `mu_*` / `sigma_*` / `w_*`, `mu_y`, `sigma_y`, `mse`, `r2`, `fallback` を含む。

| 列 | 説明 |
| --- | --- |
| `stadium` | 場名(全角:桐生・戸田 等) |
| `n_samples` | SLSQP fit に使われた行数 |
| `mu_{key}` / `sigma_{key}` | その場の 5 要素生pt値の平均と標準偏差(偏差値変換に使用) |
| `w_{key}` | その要素の重み(非負・合計 1) |
| `r2` | 着順予測の決定係数 |
| `fallback` | 1 = サンプル不足で均等重み(0.2 ずつ)に倒した |

build_index.py は実行時に **対象日の月以下で最新の重みファイル** を自動選択するため、未来日(月)用に重みファイルを事前生成しておく運用も可能。

> **用途**: index 計算の中間成果物。重みの場別比較をすると、たとえば桐生は気象pt の重みが大きい(波が立ちやすいレース場)、福岡は 枠番pt の重みが大きい(イン強度が高い)など、場の性格が数値で見える。

---

### ファイル間の関係性

```
Programs              → 選手情報・成績データ（事前情報）
     ⇅
Race Cards            → 出走表詳細（Programs と並行: 全国/当地3連率・節間14スロット成績）
     ⇅
Recent National Form  → 全国近況5節（節期間・グレード・着順時系列）
     ⇅
Recent Local Form     → 当地近況5節（同形式、当地ソースのみ）
     ⇅
Motor Stats           → モーター期成績スナップショット（場×モーター 1日1行）
     ↓
Previews              → 当日の展示会走行テスト（当朝バッチ）
     ↓
Realtime Preview      → 締切5分前の直前情報（tkz / stt / sui / original_exhibition の per-source 追記）
     ↓
Strength Index        → 派生:5要素を場別重みで線形結合した強さポイント
     ↓
Results               → 本レースの結果（事後情報）

Stadium Parameters    → win_rate.csv / sui_params.csv / index_weights/*.csv (Index 計算の参照テーブル)
```

**基本的な追跡方法**: 同じ `レースコード` で各ファイルを紐付けることで、レースの事前情報から当日の展示・直前情報、最終結果までを一貫して追跡できます。

**例**: レースコード「202602092301」で検索すると：
1. **Programs** から → 参加選手のプロフィール・成績
2. **Race Cards** から → 全国/当地3連率・節間14スロット成績
3. **Recent National Form / Recent Local Form** から → 全国・当地の直近5節成績
4. **Motor Stats** から → モーター期成績スナップショット
5. **Previews** から → 展示会での走行タイム・当日コンディション
6. **Realtime Preview** から → 締切5分前の直前スナップショット（時系列で複数ソース）
7. **Strength Index** から → 6 枠分の強さポイント(偏差値)と要素別寄与の内訳
8. **Results** から → 最終順位・払戻金・実際の進入コース・ST

これらを組み合わせることで、レースの準備段階から当日の直前情報・予測値・実結果までを一貫して追跡でき、特徴量設計や分析に活用できます。


## Quick Start

### Prerequisites

- Python 3.8+
- git
- pip (included with Python)

### Installation

```bash
# Clone repository
git clone https://github.com/your-org/boatrace-data.git
cd boatrace-data

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r scripts/requirements.txt
```

### Daily Sync (Test Run)

```bash
# Fetch yesterday's results and today's program
python scripts/fetch-and-convert.py --dry-run

# Output:
# [2025-12-01 15:10:05] Starting fetch-and-convert (mode: daily, dry-run: true)
# [2025-12-01 15:10:05] Processing dates: 2025-11-30 to 2025-12-01
# ... (processing logs)
# [2025-12-01 15:10:52] ✓ COMPLETED SUCCESSFULLY (dry-run - no files written)
```

## Project Structure

```
scripts/
├── fetch-and-convert.py         # Main entry point (legacy daily fetch)
├── preview-realtime.py          # Realtime preview scraper (also updates index)
├── build_index.py               # Strength Index builder (--mode daily/realtime, --update-races)
├── build_weights.py             # Monthly weight learner (per-stadium 5-feature weights)
├── build_sui_params.py          # 24-stadium weather coefficient learner
├── boatrace/                    # Python package
│   ├── __init__.py
│   ├── downloader.py            # HTTP downloads with retry
│   ├── extractor.py             # LZH decompression
│   ├── parser.py                # Fixed-width text parsing
│   ├── converter.py             # Text → CSV conversion
│   ├── storage.py               # File I/O operations
│   ├── git_operations.py        # Git commit/push operations
│   ├── index_features.py        # Shared 5-feature computation (build_index/build_weights)
│   └── logger.py                # Structured JSON logging
├── requirements.txt
└── tests/
    ├── unit/
    └── integration/

.github/workflows/
├── daily-sync.yml               # Daily data sync + daily index batch (00:10 JST)
├── preview-realtime.yml         # Realtime preview (every minute, 08:30-23:00 JST)
└── monthly-weights.yml          # Monthly weight rebuild (1st of month, 09:00 JST)

infra/                           # Cloud Run Jobs deployment for preview-realtime
├── Dockerfile
├── run.sh
├── cloudbuild.yaml
└── README.md                    # Setup/update procedures for GCP

data/                            # Published data (created at runtime)
├── programs/YYYY/MM/DD.csv
├── programs/race_cards/YYYY/MM/DD.csv
├── programs/recent_national/YYYY/MM/DD.csv
├── programs/recent_local/YYYY/MM/DD.csv
├── programs/motor_stats/YYYY/MM/DD.csv
├── previews/
│   ├── YYYY/MM/DD.csv                      # daily combined (preview.py)
│   ├── tkz/YYYY/MM/DD.csv                  # realtime: 体重・展示タイム・チルト
│   ├── stt/YYYY/MM/DD.csv                  # realtime: 進入コース・スタート展示
│   ├── sui/YYYY/MM/DD.csv                  # realtime: 水面気象スナップショット
│   └── original_exhibition/YYYY/MM/DD.csv  # realtime: オリジナル展示
├── index/YYYY/MM/DD.csv                    # 派生: 強さポイント (5要素偏差値+寄与+合計)
├── estimate/
│   └── stadium/
│       ├── win_rate.csv                    # 場×季節×コース勝率
│       ├── sui_params.csv                  # 24場気象線形回帰パラメータ
│       └── index_weights/YYYY-MM.csv       # 月次重み(直近6ヶ月で再学習)
└── results/YYYY/MM/DD.csv

.boatrace/
└── config.json                  # Configuration

logs/
└── boatrace-YYYY-MM-DD.json    # Execution logs
```

## Usage

### Fetch and Convert Daily Data

```bash
# Default: fetch yesterday's results and today's program
python scripts/fetch-and-convert.py

# Specific date range
python scripts/fetch-and-convert.py \
  --start-date 2025-12-01 \
  --end-date 2025-12-03

# Force overwrite existing files
python scripts/fetch-and-convert.py \
  --start-date 2025-12-01 \
  --end-date 2025-12-01 \
  --force

# Dry run (no files written)
python scripts/fetch-and-convert.py \
  --start-date 2025-12-01 \
  --dry-run
```

### Run Realtime Preview Scraper

```bash
# Default: target today (JST), eligibility window = [now+1min, now+10min]
python scripts/preview-realtime.py

# Plan only — log eligible races but write nothing
python scripts/preview-realtime.py --dry-run

# Write CSVs but skip git commit & push
python scripts/preview-realtime.py --no-commit

# Override the reference time (HH:MM JST), useful for testing
python scripts/preview-realtime.py --now 12:30 --no-commit

# Wider eligibility window (override defaults)
python scripts/preview-realtime.py --window-min 2 --window-max 15
```

Designed to run every minute via `.github/workflows/preview-realtime.yml`. On each invocation it:

1. Fetches `https://race.boatcast.jp/api_txt/getHoldingList2_{YYYYMMDD}.json` to discover open venues + per-race deadline times (no caching, no persistence).
2. Selects races whose deadline falls in the eligibility window AND that are not yet recorded in every per-source CSV.
3. Scrapes `bc_j_tkz` / `bc_j_stt` / `bc_sui` / `bc_oriten` for each eligible race and appends one row per source.
4. Commits & pushes the changes (single commit per invocation; nothing is committed when no rows were appended).

Idempotency is per-source: if `tkz` succeeds but `stt` is still missing for race X, the next minute's run only retries `stt` for X.

### Scrape Race Card Detail Data

```bash
# Default: scrape yesterday's race-card data (JST)
python scripts/race-card.py

# Specific date
python scripts/race-card.py --date 2026-04-25

# Dry run (no file written, no git push)
python scripts/race-card.py --date 2026-04-25 --dry-run

# Force overwrite existing CSV
python scripts/race-card.py --date 2026-04-25 --force
```

Data source: `race.boatcast.jp` の per-race TSV (`/hp_txt/{jo}/bc_j_str3_*.txt`). The script uses the same-day B-file from `mbrace.or.jp` to determine which races are scheduled (matching `original-exhibition.py`'s flow). Available approximately from **2025-05-02 onwards**.

### Scrape Recent Form Data (全国・当地近況5節)

```bash
# Default: scrape yesterday's recent-form data (JST)
python scripts/recent-form.py

# Specific date
python scripts/recent-form.py --date 2026-04-25

# Dry run (no files written, no git push)
python scripts/recent-form.py --date 2026-04-25 --dry-run

# Force overwrite both CSV files
python scripts/recent-form.py --date 2026-04-25 --force
```

A single run produces both `data/programs/recent_national/YYYY/MM/DD.csv` and `data/programs/recent_local/YYYY/MM/DD.csv` from `bc_zensou` and `bc_zensou_touchi` respectively. The B-file from `mbrace.or.jp` is used to look up which racer is in which boat at each race. Per-stadium fetch only — at most ~48 boatcast requests per day even on 24-stadium peak days.

### Scrape Motor Stats Data (モーター期成績)

```bash
# Default: scrape yesterday's motor stats (JST)
python scripts/motor-stats.py

# Specific date
python scripts/motor-stats.py --date 2026-04-25

# Dry run (no file written, no git push)
python scripts/motor-stats.py --date 2026-04-25 --dry-run

# Force overwrite existing CSV
python scripts/motor-stats.py --date 2026-04-25 --force
```

The script fetches `bc_mst` (motor period start date) and `bc_mdc` (per-motor stats) from `race.boatcast.jp` for every stadium that has races on the given date (per the same-day B-file from `mbrace.or.jp`). All motors are written to a single CSV at `data/programs/motor_stats/YYYY/MM/DD.csv`.

**Backfill is not possible** — race.boatcast.jp only exposes the current motor period for each stadium, so historical periods are lost. Run this script daily going forward to accumulate time-series snapshots.

### Backfill Race Card Data

`race.boatcast.jp` carries `bc_j_str3` from **2025-05-03** onwards. Use `backfill-race-card.py` to fetch a date range at once.

```bash
# Default: from 2025-05-03 to yesterday, skipping dates whose CSV already exists
python scripts/backfill-race-card.py

# Narrower range (inclusive)
python scripts/backfill-race-card.py \
  --start-date 2025-05-03 --end-date 2025-05-31

# Overwrite existing CSVs
python scripts/backfill-race-card.py \
  --start-date 2025-05-03 --end-date 2025-05-31 --force

# Dry run (fetch/parse but do not write files or push git)
python scripts/backfill-race-card.py --dry-run

# Commit & push each day as it completes (default OFF — backfills stay local)
python scripts/backfill-race-card.py --push

# Print a progress line every day (default: every 10 days)
python scripts/backfill-race-card.py --progress-every 1
```

Characteristics:

- **Resumable**: by default, any date whose CSV already exists under `data/programs/race_cards/YYYY/MM/DD.csv` is skipped. Re-running after an interruption picks up where it left off.
- **Rate-limited**: respects `rate_limit_interval_seconds` in `.boatrace/config.json`. With ~288 race-level requests per peak day, a full backfill (~12 months ≒ 360 days) takes several hours at the default 1s interval.
- **No automatic git push**: default does not push anything. Use `--push` for per-day pushes, or run `git add data/programs/race_cards && git commit && git push` manually once the run is complete.
- **Earliest-date guard**: starting earlier than 2025-05-03 is allowed but the script warns, and those days are recorded as "no_races".

### Build Strength Index (強さポイント)

```bash
# 当日朝に走らせる日次バッチ:
#   枠番・選手・モーター + 暫定強さpt を埋める。展示・気象は 50 で補完。
python scripts/build_index.py --date 2026-05-03 --mode daily

# 過去日のバックフィル(全要素揃った状態で計算):
python scripts/build_index.py --date 2026-05-03 --mode realtime

# 一部レースだけ展示・気象を再計算して状態を realtime に更新
# (preview-realtime.py から内部呼び出しされる)
python scripts/build_index.py --date 2026-05-03 \
  --update-races 202605030101,202605030102

# 過去月のバックフィル例(月毎に重みファイルが必要):
for d in $(seq -w 1 31); do
  python scripts/build_index.py --date 2026-05-${d} --mode realtime
done
```

### Build Monthly Weights (場別重み)

```bash
# 対象月の重みを直近6ヶ月のデータから学習
python scripts/build_weights.py --month 2026-05

# 過去月の重みを生成(walk-forward 検証用)
python scripts/build_weights.py --month 2026-04
python scripts/build_weights.py --month 2026-03
```

学習窓は `[対象月 - 6ヶ月, 対象月 - 1日]`(対象月のデータは含まない=リーケージなし)。場ごとに非負・合計1の制約で SLSQP 最適化。motor_stats の収録履歴が短い場合、motor 重みは小さくなる傾向あり。

### Build Stadium Weather Params (sui_params.csv)

```bash
# 24場分の気象線形回帰パラメータを実データから再学習
python scripts/build_sui_params.py \
  --start-date 2025-01-01 --end-date 2026-04-30 \
  --out data/estimate/stadium/sui_params.csv
```

`previews + results` を結合して場×コース別に線形回帰し、波・風(追い/向かい)・気温水温差・天候から有利pt変動を推定。

### Backfill Recent Form Data

`race.boatcast.jp` carries `bc_zensou` and `bc_zensou_touchi` from **2024-03-12** onwards. Use `backfill-recent-form.py` to populate both `data/programs/recent_national/` and `data/programs/recent_local/` for a date range.

```bash
# Default: from 2024-03-12 to yesterday, skipping dates whose national CSV exists
python scripts/backfill-recent-form.py

# Narrower range
python scripts/backfill-recent-form.py \
  --start-date 2024-03-12 --end-date 2024-03-31

# Overwrite both CSVs
python scripts/backfill-recent-form.py \
  --start-date 2024-03-12 --end-date 2024-03-31 --force

# Dry run
python scripts/backfill-recent-form.py --dry-run

# Per-day push (commits both national + local CSVs per day)
python scripts/backfill-recent-form.py --push

# More frequent progress
python scripts/backfill-recent-form.py --progress-every 1
```

Characteristics:

- **Resumable**: skip-if-exists is keyed off `data/programs/recent_national/YYYY/MM/DD.csv`. If only `recent_local` is missing on a particular day, run with `--force` to regenerate both.
- **Fast**: only 2 boatcast requests per stadium per day (one bc_zensou + one bc_zensou_touchi). Even a 24-stadium peak day is ~48 requests, so a full ~25-month backfill completes in well under an hour at the default rate-limit.
- **Earliest-date guard**: starts before 2024-03-12 are allowed but the script warns, and those days record "no_races".

## Testing

```bash
# Run all unit tests
pytest tests/unit/

# Run specific test file
pytest tests/unit/test_parser.py

# Run with coverage
pytest --cov=boatrace tests/unit/
```

## Environment Setup for GitHub Actions

1. Repository secrets (configured in GitHub):
   - `GITHUB_TOKEN` (provided automatically)
   - Optional: `GIT_USER_EMAIL` (defaults to "action@github.com")
   - Optional: `GIT_USER_NAME` (defaults to "GitHub Action")

2. GitHub Pages configuration:
   - Settings → Pages → Source: Deploy from a branch
   - Branch: `main`
   - Folder: `/ (root)`

### Workflows

- **`daily-sync.yml`** — Runs every day at 00:10 JST. Processes Results, Programs, Previews, Race Cards, Recent Form, and Motor Stats for the previous day. Then runs **Build Daily Index Batch** (`build_index.py --mode daily`) to populate today's `data/estimate/index/YYYY/MM/DD.csv` with 枠番・選手・モーター + 暫定強さpt(状態 = `daily`、展示・気象は 50 で補完)。Each step uses `if: always()` (and `continue-on-error: true` for third-party-source steps) so a single source outage does not break the rest of the pipeline.
- **`preview-realtime.yml`** — Runs every minute between JST 08:30 and 23:00. Scrapes per-source preview data (`tkz` / `stt` / `sui` / `original_exhibition`) for races whose deadline falls in the eligibility window (default `[now+1min, now+10min]`). After appending preview rows, **also updates the corresponding rows in `data/estimate/index/YYYY/MM/DD.csv`** (展示・気象 を実値で再計算 → 状態 = `realtime`)、both changes go in a single commit. Idempotent and resilient to cron drift; commits one batch per invocation only when rows are actually appended.
  - **Cloud Run Jobs 移行版**: GitHub Actions の cron が間引かれる課題に対応するため、Cloud Scheduler + Cloud Run Jobs で同じ `preview-realtime.py` を 5 分粒度で確実に実行する構成も用意しています。詳細は [`infra/README.md`](infra/README.md) を参照。
- **`monthly-weights.yml`** — Runs on the 1st of each month at 09:00 JST. Re-learns 24-stadium × 5-feature weights from the prior 6 months of data and writes `data/estimate/stadium/index_weights/YYYY-MM.csv`. `build_index.py` automatically picks up the latest weights ≤ the target month.

## Configuration

Edit `.boatrace/config.json` to customize:

```json
{
  "rate_limit_interval_seconds": 3,
  "max_retries": 3,
  "initial_backoff_seconds": 5,
  "max_backoff_seconds": 30,
  "request_timeout_seconds": 30,
  "log_level": "INFO",
  "log_file": "logs/boatrace-{DATE}.json"
}
```

## Performance

- **Daily execution**: ~10-15 seconds (typical)
- **Historical backfill (3 years)**: ~60 minutes
- **CSV file size**: 100-500 KB per file

## Data Source

Official Boatrace Races Server: http://www1.mbrace.or.jp/od2/

## License

MIT License