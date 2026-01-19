# Boatrace Data Automation

ボートレースのデータは、独自フォーマットで分散しており、収集と整形に時間がかかります。
そこで、機械学習で利用しやすいように1レース1行のCSVファイルを作成しました。
httpsでダウンロードできるため、Agentからのアクセスにも利用しやすくなっています。
更新は1日1回です。
最新の情報が必要な場合、[Boatrace OpenAPI](https://github.com/BoatraceOpenAPI) などの別のソースをご利用ください。

## データファイル

毎日、以下の5つのCSVファイルが自動生成されます。各ファイルはレースの異なる段階のデータを含みます。

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

**環境・気象情報**（レース当日のコンディション）:
- `風速(m)` (3.0): 風速（m/s）
- `風向` (1): 風向（コード値）
- `波の高さ(cm)` (5.0): 波の高さ（cm）
- `天候` (1): 天気（コード値。1=晴、2=曇、3=雨など）
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

### Estimates (レース予想)
**ファイルパス**: `data/estimate/YYYY/MM/DD.csv`
**URL**: https://boatracecsv.github.io/data/estimate/2026/01/01.csv

Programs と Previews データを用いて、機械学習モデル（Random Forest）により予想した各レースの1着、2着、3着の艇番です。

#### サンプルデータ（1行目）
```
レースコード,予想1着,予想2着,予想3着
202212230201,2,1,4
```

#### 列の詳細説明

**基本情報**:
- `レースコード` (202212230201): Programs・Previews・Results と同じ形式のレース識別子
- `予想1着` (2): 1着になると予想された艇番
- `予想2着` (1): 2着になると予想された艇番
- `予想3着` (4): 3着になると予想された艇番

**予想ロジック**:
- 過去30日間のデータで Random Forest モデルを訓練
- Features: 選手の全国勝率、2連対率、当地勝率、モーター・ボート成績、展示会での走行データ
- 各艇の予想着順をスコアリングして、上位3艇を抽出

> **用途**: レース予想の統計的基礎データ、予想精度の検証、機械学習モデルの評価

---

### Confirmations (予想確認)
**ファイルパス**: `data/confirm/YYYY/MM/DD.csv`
**URL**: https://boatracecsv.github.io/data/confirm/2026/01/01.csv

Estimates の予想結果と Results の実際のレース結果を比較し、予想の的中状況を記録したファイルです。

#### サンプルデータ（1行目）
```
レースコード,予想1着,予想2着,予想3着,実際1着,実際2着,実際3着,1着的中,2着的中,3着的中,全的中
202212310211,1,3,2,1,3,2,○,○,○,○
```

#### 列の詳細説明

**基本情報**:
- `レースコード` (202212310211): Estimates・Results と同じ形式のレース識別子

**予想情報**:
- `予想1着` (1): Estimates から取得した予想1着艇番
- `予想2着` (3): Estimates から取得した予想2着艇番
- `予想3着` (2): Estimates から取得した予想3着艇番

**実際の結果**:
- `実際1着` (1): Results から取得した実際の1着艇番
- `実際2着` (3): Results から取得した実際の2着艇番
- `実際3着` (2): Results から取得した実際の3着艇番

**的中判定**:
- `1着的中` (○): 予想1着と実際の1着が一致したか。○=一致、×=不一致
- `2着的中` (○): 予想2着と実際の2着が一致したか。○=一致、×=不一致
- `3着的中` (○): 予想3着と実際の3着が一致したか。○=一致、×=不一致
- `全的中` (○): 1着・2着・3着すべてが一致したか。○=全一致、×=いずれか一つ以上が不一致

> **用途**: 予想精度の追跡、モデルのパフォーマンス評価、統計的な検証、改善点の検出

---

### ファイル間の関係性

```
Programs      → 選手情報・成績データ（事前情報）
     ↓
Previews      → 当日の展示会走行テスト（当朝情報）
     ↓
Programs + Previews → Estimates（機械学習による予想）
     ↓
Results       → 本レースの結果（事後情報）
     ↓
Estimates + Results → Confirmations（予想の的中確認）
```

**基本的な追跡方法**: 同じ `レースコード` で5つのファイルを紐付けることで、レースの事前準備から予想、実際の結果、そして予想精度の検証までの完全な追跡が可能です。

**予想・検証フロー**:
- Programs と Previews から機械学習モデルで 1 日分の全レースを予想 → Estimates を生成
- Results が公開された後、Estimates と Results を比較 → Confirmations を生成

**例**: レースコード「202212310211」で検索すると：
1. **Programs** から → 参加選手のプロフィール・成績
2. **Previews** から → 展示会での走行タイム・当日コンディション
3. **Estimates** から → 機械学習による 1着・2着・3着の予想艇番
4. **Results** から → 最終順位・払戻金情報
5. **Confirmations** から → 予想が的中したかどうかの判定結果

これらを組み合わせることで、レースの準備段階から予想、結果、そして予想精度の分析までの全体像を把握でき、モデルの改善や投票戦略の検討に活用できます。


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
├── fetch-and-convert.py         # Main entry point
├── boatrace/                    # Python package
│   ├── __init__.py
│   ├── downloader.py            # HTTP downloads with retry
│   ├── extractor.py             # LZH decompression
│   ├── parser.py                # Fixed-width text parsing
│   ├── converter.py             # Text → CSV conversion
│   ├── storage.py               # File I/O operations
│   ├── git_operations.py        # Git commit/push operations
│   └── logger.py                # Structured JSON logging
├── requirements.txt
└── tests/
    ├── unit/
    │   ├── test_downloader.py
    │   ├── test_extractor.py
    │   ├── test_parser.py
    │   ├── test_converter.py
    │   └── test_storage.py
    ├── integration/
    │   ├── test_end_to_end.py
    │   └── fixtures/
    └── conftest.py

.github/workflows/
└── daily-sync.yml               # GitHub Actions workflow (00:10 JST daily)

data/                            # Published data (created at runtime)
├── results/
│   └── YYYY/MM/DD.csv
└── programs/
    └── YYYY/MM/DD.csv

.boatrace/
└── config.json                  # Configuration

logs/
└── boatrace-YYYY-MM-DD.json    # Execution logs
```

## Documentation

- **[Specification](specs/001-boatrace-automation/spec.md)** - Functional requirements and design decisions
- **[Implementation Plan](specs/001-boatrace-automation/plan.md)** - Technical architecture and project structure
- **[Data Model](specs/001-boatrace-automation/data-model.md)** - Entity definitions and relationships
- **[CLI Contract](specs/001-boatrace-automation/contracts/cli.md)** - Command-line interface specification
- **[GitHub Actions Contract](specs/001-boatrace-automation/contracts/github-actions.md)** - Workflow specification
- **[Quickstart Guide](specs/001-boatrace-automation/quickstart.md)** - Developer guide and troubleshooting

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

## Troubleshooting

See [Quickstart Guide - Troubleshooting](specs/001-boatrace-automation/quickstart.md#troubleshooting) for common issues and solutions.

## License

MIT License