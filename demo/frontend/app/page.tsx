"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import { Header } from "@/components/header"
import { ControlBar } from "@/components/control-bar"
import { ExperimentCard } from "@/components/experiment-card"
import { MetricsChart } from "@/components/metrics-chart"
import { ResultsTable } from "@/components/results-table"
import { LoadingState } from "@/components/loading-state"
import { ErrorState } from "@/components/error-state"
import { EmptyState } from "@/components/empty-state"
import { API_BASE_URL, getExperimentResults, getRunMetrics, listExperiments } from "@/lib/api"
import type { Experiment, RunMetric, ResultsResponse } from "@/types/api"

const isAbortError = (error: unknown) => {
  if (typeof DOMException !== "undefined" && error instanceof DOMException) {
    return error.name === "AbortError"
  }
  return error instanceof Error && error.name === "AbortError"
}

export default function Page() {
  const [experiments, setExperiments] = useState<Experiment[]>([])
  const [selectedExperimentId, setSelectedExperimentId] = useState<number | null>(null)
  const [selectedRunId, setSelectedRunId] = useState<number | null>(null)
  const [selectedK, setSelectedK] = useState<number>(50)

  const [metrics, setMetrics] = useState<RunMetric[]>([])
  const [results, setResults] = useState<ResultsResponse | null>(null)

  const [isLoadingExperiments, setIsLoadingExperiments] = useState(true)
  const [isLoadingMetrics, setIsLoadingMetrics] = useState(false)
  const [isLoadingResults, setIsLoadingResults] = useState(false)

  const [experimentsError, setExperimentsError] = useState<string | null>(null)
  const [metricsError, setMetricsError] = useState<string | null>(null)
  const [resultsError, setResultsError] = useState<string | null>(null)

  const [metricsReloadKey, setMetricsReloadKey] = useState(0)
  const [resultsReloadKey, setResultsReloadKey] = useState(0)

  const selectedExperiment = useMemo(
    () => experiments.find((exp) => exp.id === selectedExperimentId) ?? null,
    [experiments, selectedExperimentId],
  )

  const loadExperiments = useCallback(async () => {
    setIsLoadingExperiments(true)
    setExperimentsError(null)
    try {
      const data = await listExperiments()
      setExperiments(data)

      if (data.length === 0) {
        setSelectedExperimentId(null)
        setSelectedRunId(null)
        setMetrics([])
        setResults(null)
        return
      }

      setSelectedExperimentId((currentId) => {
        const nextId = currentId && data.some((exp) => exp.id === currentId) ? currentId : data[0].id
        const experiment = data.find((exp) => exp.id === nextId)

        setSelectedRunId((currentRunId) => {
          if (!experiment || experiment.run_ids.length === 0) {
            return null
          }

          if (currentRunId && experiment.run_ids.includes(currentRunId)) {
            return currentRunId
          }

          return experiment.run_ids[experiment.run_ids.length - 1] ?? null
        })

        return nextId
      })
    } catch (error) {
      console.error(error)
      setExperimentsError("Failed to load experiments. Please retry.")
    } finally {
      setIsLoadingExperiments(false)
    }
  }, [])

  useEffect(() => {
    void loadExperiments()
  }, [loadExperiments])

  useEffect(() => {
    if (!selectedRunId) {
      setIsLoadingMetrics(false)
      setMetricsError(null)
      setMetrics([])
      return
    }

    const controller = new AbortController()
    setIsLoadingMetrics(true)
    setMetricsError(null)

    getRunMetrics(selectedRunId, { signal: controller.signal })
      .then((data) => {
        setMetrics(data)
      })
      .catch((error) => {
        if (isAbortError(error)) {
          return
        }
        console.error(error)
        setMetricsError("Failed to load metrics.")
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setIsLoadingMetrics(false)
        }
      })

    return () => {
      controller.abort()
    }
  }, [selectedRunId, metricsReloadKey])

  useEffect(() => {
    setResults(null)

    if (!selectedExperimentId || !selectedRunId) {
      setIsLoadingResults(false)
      setResultsError(null)
      return
    }

    const controller = new AbortController()
    setIsLoadingResults(true)
    setResultsError(null)

    getExperimentResults(selectedExperimentId, {
      runId: selectedRunId,
      k: selectedK,
      signal: controller.signal,
    })
      .then((data) => {
        setResults(data)
      })
      .catch((error) => {
        if (isAbortError(error)) {
          return
        }
        console.error(error)
        setResultsError("Failed to load results.")
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setIsLoadingResults(false)
        }
      })

    return () => {
      controller.abort()
    }
  }, [selectedExperimentId, selectedRunId, selectedK, resultsReloadKey])

  const handleExperimentChange = (experiment: Experiment) => {
    setSelectedExperimentId(experiment.id)
    const nextRun = experiment.run_ids.length > 0 ? experiment.run_ids[experiment.run_ids.length - 1] : null
    setSelectedRunId(nextRun)
    setSelectedK(50)
    setMetrics([])
    setResults(null)
    setMetricsError(null)
    setResultsError(null)
  }

  const handleRunChange = (runId: number) => {
    setSelectedRunId(runId)
    setMetrics([])
    setResults(null)
    setMetricsError(null)
    setResultsError(null)
    setMetricsReloadKey((value) => value + 1)
    setResultsReloadKey((value) => value + 1)
  }

  const handleKChange = (k: number) => {
    const clamped = Math.max(1, Math.min(1000, k))
    setSelectedK(clamped)
    setResults(null)
    setResultsReloadKey((value) => value + 1)
  }

  const retryMetrics = () => {
    if (!selectedRunId) {
      return
    }
    setMetricsReloadKey((value) => value + 1)
  }

  const retryResults = () => {
    if (!selectedExperimentId || !selectedRunId) {
      return
    }
    setResultsReloadKey((value) => value + 1)
  }

  return (
    <div className="min-h-screen bg-background">
      <Header apiBase={API_BASE_URL} />
      <main className="container mx-auto px-4 py-6 space-y-6">
        {isLoadingExperiments ? (
          <LoadingState />
        ) : experimentsError ? (
          <ErrorState message={experimentsError} onRetry={() => void loadExperiments()} />
        ) : experiments.length === 0 ? (
              <EmptyState message="No experiments available." />
        ) : (
          <>
            <ControlBar
              experiments={experiments}
              selectedExperiment={selectedExperiment}
              selectedRunId={selectedRunId}
              onExperimentChange={handleExperimentChange}
              onRunChange={handleRunChange}
            />

            {selectedExperiment && <ExperimentCard experiment={selectedExperiment} />}

            {isLoadingMetrics ? (
              <LoadingState />
            ) : metricsError ? (
              <ErrorState message={metricsError} onRetry={retryMetrics} />
            ) : metrics.length > 0 ? (
              <MetricsChart metrics={metrics} selectedK={selectedK} onKSelect={handleKChange} />
            ) : (
              <EmptyState message="No metrics found for the selected experiment." />
            )}

            {isLoadingResults ? (
              <LoadingState />
            ) : resultsError ? (
              <ErrorState message={resultsError} onRetry={retryResults} />
            ) : results ? (
              <ResultsTable results={results} experiment={selectedExperiment} />
            ) : (
              <EmptyState message="Select an experiment and run to view results." />
            )}
          </>
        )}
      </main>
    </div>
  )
}
