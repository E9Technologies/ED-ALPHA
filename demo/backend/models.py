from __future__ import annotations

from datetime import date, datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class Experiment(BaseModel):
    """Experiment header with minimal fields plus attached run IDs."""
    id: int
    predict_date: date
    horizon_days: int
    item_codes: Optional[List[str]] = None
    neg_multiplier: int
    seed: Optional[int] = None
    created_at: datetime
    run_ids: List[int] = Field(default_factory=list)


class RunMetric(BaseModel):
    """@K metrics for a run: precision/recall and the top-k arrays."""
    k: int
    top_ciks: List[int]
    top_scores: List[int]
    positives_in_top: int
    total_positives: int
    recall: float
    precision: float
    computed_at: datetime


class Evidence(BaseModel):
    """An evidence item for a company: scored article with LLM reason."""
    llm_score: int
    summary: str
    url: str
    title: Optional[str] = None
    evaluated_at: datetime


class EventInfo(BaseModel):
    """(Optional) confirmed 8-K event within the prediction window."""
    accession_number: str
    form: str
    filing_date: Optional[date] = None
    primary_document: str
    items: Optional[str] = None
    url: Optional[str] = None  # Resolved SEC EDGAR URL


class ResultRow(BaseModel):
    """A single company result row for Top-K listing."""
    cik: int
    company_name: Optional[str] = None
    total_score: int
    evidence: List[Evidence] = Field(default_factory=list)
    event: Optional[EventInfo] = None  # None when no confirmed event


class ResultsResponse(BaseModel):
    """The full result payload for an experiment+run at Top-K."""
    experiment_id: int
    run_id: int
    k: int
    results: List[ResultRow]
