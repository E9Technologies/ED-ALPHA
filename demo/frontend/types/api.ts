export type Experiment = {
  id: number
  predict_date: string | Date
  horizon_days: number
  item_codes: string[]
  neg_multiplier: number
  seed: number
  created_at: string | Date
  run_ids: number[]
}

export type RunMetric = {
  k: number
  top_ciks: number[]
  top_scores: number[]
  positives_in_top: number
  total_positives: number
  recall: number
  precision: number
  computed_at: string | Date
}

export type Evidence = {
  llm_score: number
  summary: string
  url: string
  title: string
  evaluated_at: string | Date
}

export type EventItems = string[] | string | null | undefined

export type EventInfo = {
  accession_number?: string | null
  form?: string | null
  filing_date?: string | Date | null
  primary_document?: string | null
  items?: EventItems
  url?: string | null
}

export type ResultRow = {
  cik: number
  company_name?: string | null
  total_score: number
  evidence: Evidence[]
  event?: EventInfo | null
}

export type ResultsResponse = {
  experiment_id: number
  run_id: number
  k: number
  results: ResultRow[]
}
