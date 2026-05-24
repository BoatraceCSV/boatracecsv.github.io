"""予想者(predictor)パッケージ。

詳細は ``registry`` モジュールを参照。
"""
from .registry import (  # noqa: F401
    COMPONENT_LABELS_REGISTRY,
    COMPONENT_MISSING_FALLBACK,
    COMPONENT_MISSING_FALLBACK_DEFAULT,
    PREDICTORS,
    STATUS_ACTIVE,
    STATUS_RETIRED,
    PredictorSpec,
    active_predictors,
    all_predictors,
    component_label,
    component_missing_fallback,
    predictor_by_id,
)
