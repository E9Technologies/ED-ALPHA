"use client"

import Link from "next/link"
import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { ExternalLink, MousePointerClick, Video } from "lucide-react"
import { Header } from "@/components/header"
import { BenchmarkOverview } from "@/components/benchmark-overview"
import { ControlBar } from "@/components/control-bar"
import { ExperimentCard } from "@/components/experiment-card"
import { MetricsChart } from "@/components/metrics-chart"
import { ResultsTable } from "@/components/results-table"
import { LoadingState } from "@/components/loading-state"
import { ErrorState } from "@/components/error-state"
import { EmptyState } from "@/components/empty-state"
import { TutorialTour } from "@/components/tutorial-tour"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { getExperimentResults, getRunMetrics, listExperiments } from "@/lib/api"
import type { Experiment, RunMetric, ResultsResponse } from "@/types/api"

const GITHUB_URL = "https://github.com/E9Technologies/ED-ALPHA"
const GITHUB_ICON_PATH = `${process.env.NEXT_PUBLIC_BASE_PATH ?? ""}/demo_walkthrough/github.png`

const isAbortError = (error: unknown) => {
  if (typeof DOMException !== "undefined" && error instanceof DOMException) {
    return error.name === "AbortError"
  }
  return error instanceof Error && error.name === "AbortError"
}

type DashboardPageProps = {
  autoOpenTutorial?: boolean
  forceExpandFirstEvidence?: boolean
}

export function DashboardPage({ autoOpenTutorial = true, forceExpandFirstEvidence = false }: DashboardPageProps = {}) {
  const [experiments, setExperiments] = useState<Experiment[]>([])
  const [selectedExperimentId, setSelectedExperimentId] = useState<number | null>(null)
  const [selectedRunId, setSelectedRunId] = useState<number | null>(null)
  const [selectedK, setSelectedK] = useState<number>(25)

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
  const [isTutorialOpen, setIsTutorialOpen] = useState(false)
  const [isTutorialChoiceOpen, setIsTutorialChoiceOpen] = useState(false)
  const [activeTutorialStep, setActiveTutorialStep] = useState<string | null>(null)
  const hasAutoOpenedTutorial = useRef(false)

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
    setSelectedK(25)
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

  const isDashboardReady = Boolean(
    !isLoadingExperiments &&
    !experimentsError &&
    experiments.length > 0 &&
    selectedExperiment &&
    !isLoadingMetrics &&
    !metricsError &&
    metrics.length > 0 &&
    !isLoadingResults &&
    !resultsError &&
    results,
  )

  useEffect(() => {
    if (!autoOpenTutorial || !isDashboardReady || hasAutoOpenedTutorial.current) {
      return
    }

    hasAutoOpenedTutorial.current = true
    setIsTutorialChoiceOpen(true)
  }, [autoOpenTutorial, isDashboardReady])

  const openTutorial = () => {
    setIsTutorialChoiceOpen(true)
  }

  const startTutorial = () => {
    setIsTutorialChoiceOpen(false)
    setIsTutorialOpen(true)
  }

  const closeTutorial = () => {
    setIsTutorialOpen(false)
    setActiveTutorialStep(null)
  }

  return (
    <div className="min-h-screen bg-background">
      <Header onTutorialOpen={openTutorial} />
      <main className="container mx-auto px-4 py-6 space-y-6" data-tour-id="dashboard-overview">
        {isLoadingExperiments ? (
          <LoadingState />
        ) : experimentsError ? (
          <ErrorState message={experimentsError} onRetry={() => void loadExperiments()} />
        ) : experiments.length === 0 ? (
          <EmptyState message="No experiments available." />
        ) : (
          <>
            <BenchmarkOverview />

            <ControlBar
              experiments={experiments}
              selectedExperiment={selectedExperiment}
              selectedRunId={selectedRunId}
              onExperimentChange={handleExperimentChange}
              onRunChange={handleRunChange}
            />

            {selectedExperiment && (
              <ExperimentCard
                experiment={selectedExperiment}
                displayNumber={experiments.indexOf(selectedExperiment) + 1}
              />
            )}

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
              <ResultsTable
                results={results}
                experiment={selectedExperiment}
                expandFirstEvidence={
                  forceExpandFirstEvidence || (isTutorialOpen && activeTutorialStep === "top-k-ranking")
                }
              />
            ) : (
              <EmptyState message="Select an experiment and run to view results." />
            )}
          </>
        )}
      </main>
      <Dialog open={isTutorialChoiceOpen} onOpenChange={setIsTutorialChoiceOpen}>
        <DialogContent className="sm:max-w-xl">
          <DialogHeader>
            <DialogTitle>Choose how to explore ED-ALPHA</DialogTitle>
            <DialogDescription>
              Start the guided dashboard tutorial, watch the timed walkthrough, or run the benchmark from GitHub.
            </DialogDescription>
          </DialogHeader>
          <div className="grid gap-3">
            <Button
              type="button"
              size="lg"
              className="h-auto justify-start whitespace-normal px-4 py-4 text-left"
              onClick={startTutorial}
            >
              <MousePointerClick className="size-5" />
              <span className="grid gap-1">
                <span className="font-semibold">Start tutorial</span>
                <span className="text-xs font-normal opacity-85">Try it with the demo user interface</span>
              </span>
            </Button>

            <Button
              asChild
              variant="outline"
              size="lg"
              className="h-auto justify-start whitespace-normal px-4 py-4 text-left"
            >
              <Link href="/walkthrough">
                <Video className="size-5" />
                <span className="grid gap-1">
                  <span className="font-semibold">View walkthrough</span>
                  <span className="text-xs font-normal text-muted-foreground">Watch the timed walkthrough</span>
                </span>
              </Link>
            </Button>

            <Button
              asChild
              variant="outline"
              size="lg"
              className="h-auto justify-start whitespace-normal px-4 py-4 text-left"
            >
              <a href={GITHUB_URL} target="_blank" rel="noreferrer">
                <img src={GITHUB_ICON_PATH} alt="" className="size-5 shrink-0" />
                <span className="grid gap-1">
                  <span className="font-semibold">Go to GitHub</span>
                  <span className="text-xs font-normal text-muted-foreground">
                    Run ED-ALPHA and submit your run
                  </span>
                </span>
                <ExternalLink className="ml-auto size-4" />
              </a>
            </Button>
          </div>
        </DialogContent>
      </Dialog>
      <TutorialTour open={isTutorialOpen} onClose={closeTutorial} onStepChange={setActiveTutorialStep} />
    </div>
  )
}
