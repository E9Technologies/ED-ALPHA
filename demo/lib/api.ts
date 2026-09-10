import snapshot from "@/data/demo-data.json"
import type { Experiment, ResultRow, ResultsResponse, RunMetric, RunSummary } from "@/types/api"

type FetchOptions = {
  signal?: AbortSignal
  query?: Record<string, string | number | boolean | undefined | null>
}

type SnapshotRun = RunSummary & {
  metrics: RunMetric[]
  topResults: ResultRow[]
}

type SnapshotShape = {
  experiment: Omit<Experiment, "runs">
  runs: SnapshotRun[]
}

const data = snapshot as SnapshotShape

const cloneJson = <T>(value: T): T => JSON.parse(JSON.stringify(value)) as T

function assertNotAborted(signal?: AbortSignal) {
  if (signal?.aborted) {
    throw new DOMException("The operation was aborted.", "AbortError")
  }
}

function getRun(runId: number): SnapshotRun {
  const run = data.runs.find((item) => item.id === runId)
  if (!run) {
    throw new Error(`runId=${runId} not found in snapshot`)
  }
  return run
}

export async function listExperiments(options?: FetchOptions): Promise<Experiment[]> {
  assertNotAborted(options?.signal)
  return [{
    ...cloneJson(data.experiment),
    runs: data.runs.map(({ id, model_name, model_label }) => ({ id, model_name, model_label })),
  }]
}

export async function getRunMetrics(runId: number, options?: FetchOptions): Promise<RunMetric[]> {
  if (!runId) {
    throw new Error("runId is required")
  }

  assertNotAborted(options?.signal)
  return cloneJson(getRun(runId).metrics)
}

type ResultsParams = {
  runId: number
  k: number
  evidencePerCompany?: number
}

export async function getExperimentResults(
  experimentId: number,
  { runId, k, evidencePerCompany = 3, ...options }: ResultsParams & FetchOptions,
): Promise<ResultsResponse> {
  if (!experimentId) {
    throw new Error("experimentId is required")
  }
  if (!runId) {
    throw new Error("runId is required")
  }

  assertNotAborted(options?.signal)

  if (experimentId !== data.experiment.id) {
    throw new Error(`experimentId=${experimentId} not found in snapshot`)
  }

  const run = getRun(runId)
  const normalizedK = Math.max(1, Math.min(k, run.topResults.length))
  const results = run.topResults.slice(0, normalizedK).map((row) => ({
    ...cloneJson(row),
    evidence: row.evidence.slice(0, evidencePerCompany).map((evidence) => cloneJson(evidence)),
  }))

  return {
    experiment_id: experimentId,
    run_id: runId,
    model_name: run.model_name,
    model_label: run.model_label,
    aggregation: "mean_article_score",
    k: normalizedK,
    results,
  }
}
