import type { Experiment, ResultsResponse, RunMetric } from "@/types/api"

const DEFAULT_API_BASE_URL = "http://localhost:5000"

export const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL ?? DEFAULT_API_BASE_URL

type FetchOptions = {
  signal?: AbortSignal
  query?: Record<string, string | number | boolean | undefined | null>
}

async function fetchJson<T>(path: string, options: FetchOptions = {}): Promise<T> {
  const url = new URL(path, API_BASE_URL)

  if (options.query) {
    Object.entries(options.query).forEach(([key, value]) => {
      if (value !== undefined && value !== null) {
        url.searchParams.set(key, String(value))
      }
    })
  }

  const response = await fetch(url.toString(), {
    method: "GET",
    signal: options.signal,
  })

  if (!response.ok) {
    const text = await response.text().catch(() => "")
    throw new Error(`API request failed (${response.status} ${response.statusText}): ${text}`)
  }

  return (await response.json()) as T
}

export async function listExperiments(options?: FetchOptions): Promise<Experiment[]> {
  return fetchJson<Experiment[]>("/experiments", {
    ...options,
  })
}

export async function getRunMetrics(runId: number, options?: FetchOptions): Promise<RunMetric[]> {
  if (!runId) {
    throw new Error("runId is required")
  }
  return fetchJson<RunMetric[]>(`/runs/${runId}/metrics`, {
    ...options,
  })
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

  return fetchJson<ResultsResponse>(`/experiments/${experimentId}/results`, {
    ...options,
    query: {
      run_id: runId,
      k,
      evidence_per_company: evidencePerCompany,
      ...(options?.query ?? {}),
    },
  })
}
