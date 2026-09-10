"use client"

import { useEffect, useState } from "react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { CheckCircle2, ChevronDown, ChevronUp, Download, ExternalLink, XCircle } from "lucide-react"
import type { EventItems, Experiment, ResultsResponse } from "@/types/api"

interface ResultsTableProps {
  results: ResultsResponse
  experiment: Experiment | null
  expandFirstEvidence?: boolean
}

export function ResultsTable({ results, experiment, expandFirstEvidence = false }: ResultsTableProps) {
  const [expandedRows, setExpandedRows] = useState<Set<number>>(new Set())

  useEffect(() => {
    if (!expandFirstEvidence) {
      return
    }

    const firstRow = results.results[0]
    if (!firstRow || firstRow.evidence.length <= 1) {
      return
    }

    setExpandedRows((currentRows) => {
      if (currentRows.has(firstRow.cik)) {
        return currentRows
      }
      const nextRows = new Set(currentRows)
      nextRows.add(firstRow.cik)
      return nextRows
    })
  }, [expandFirstEvidence, results.results])

  const toggleRow = (cik: number) => {
    const newExpanded = new Set(expandedRows)
    if (newExpanded.has(cik)) {
      newExpanded.delete(cik)
    } else {
      newExpanded.add(cik)
    }
    setExpandedRows(newExpanded)
  }

  const downloadJson = () => {
    if (!experiment) {
      return
    }
    const payload = {
      experiment,
      results,
    }
    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" })
    const url = URL.createObjectURL(blob)
    const link = document.createElement("a")
    const fileExperimentId = results.experiment_id ?? experiment.id
    link.href = url
    link.download = `experiment-${fileExperimentId}-run-${results.run_id}.json`
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
    URL.revokeObjectURL(url)
  }

  const normalizeEventItems = (items: EventItems): string[] => {
    if (!items) {
      return []
    }
    if (Array.isArray(items)) {
      return items.map((item) => String(item)).filter(Boolean)
    }
    if (typeof items === "string") {
      return items
        .split(/[;,]/)
        .map((item) => item.trim())
        .filter(Boolean)
    }
    return [String(items)]
  }

  return (
    <Card data-tour-id="top-k-table">
      <CardHeader className="flex flex-row items-center justify-between">
        <div>
          <CardTitle className="text-xl">Top-{results.k} Companies</CardTitle>
          <p className="mt-1 text-sm text-muted-foreground">{results.model_label} · Ranked by average article score</p>
        </div>
        <Button
          variant="outline"
          size="sm"
          onClick={downloadJson}
          disabled={!experiment}
          data-tour-id="export-json"
        >
          <Download className="w-4 h-4 mr-2" />
          Download JSON
        </Button>
      </CardHeader>
      <CardContent>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-border">
                <th className="text-left py-3 px-2 font-medium text-muted-foreground">Rank / Company</th>
                <th className="text-left py-3 px-2 font-medium text-muted-foreground">News Signals</th>
                <th className="text-left py-3 px-2 font-medium text-muted-foreground">Outcome</th>
              </tr>
            </thead>
            <tbody>
              {results.results.map((row, rowIndex) => {
                const isExpanded = expandedRows.has(row.cik)
                const firstEvidence = row.evidence[0]
                const hasMoreEvidence = row.evidence.length > 1
                const eventItems = normalizeEventItems(row.event?.items)
                const isFirstRow = rowIndex === 0
                const isHit = row.label === 1

                return (
                  <tr
                    key={row.cik}
                    className="border-b border-border hover:bg-secondary/50"
                    data-tour-id={isFirstRow ? "top-k-first-row" : undefined}
                  >
                    <td className="py-3 px-2 align-top w-[10rem]">
                      <div className="space-y-2">
                        <div className="flex items-baseline gap-2">
                          <span className="font-mono font-semibold">#{rowIndex + 1}</span>
                          <div className="font-medium leading-snug text-foreground">{row.company_name || "N/A"}</div>
                        </div>
                        <div
                          className="font-mono text-[0.72rem] leading-none text-muted-foreground"
                          title="SEC Central Index Key"
                        >
                          CIK {row.cik}
                        </div>
                        <div className="text-xs" title={`Mean of ${row.article_count} article scores; rankings use unrounded values`}>
                          <span className="text-muted-foreground">Avg. score </span>
                          <span className="font-mono font-semibold">{row.article_count ? row.average_score.toFixed(3) : "N/A"}</span>
                          <div className="mt-1 text-muted-foreground">{row.article_count} {row.article_count === 1 ? "article" : "articles"}</div>
                        </div>
                      </div>
                    </td>
                    <td className="py-3 px-2 align-top min-w-[15rem]" data-tour-id={isFirstRow ? "top-k-evidence" : undefined}>
                      {firstEvidence ? (
                        <div className="space-y-2">
                          <div className="flex items-start gap-2">
                            <div className="flex-1">
                              <a
                                href={firstEvidence.url}
                                target="_blank"
                                rel="noopener noreferrer"
                                className="text-sm font-medium text-primary hover:underline inline-flex items-center gap-1"
                              >
                                {firstEvidence.title}
                                <ExternalLink className="w-3 h-3 shrink-0" />
                              </a>
                              <p className="mt-1 text-xs text-muted-foreground line-clamp-3">{firstEvidence.summary}</p>
                            </div>
                          </div>
                          {hasMoreEvidence && (
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => toggleRow(row.cik)}
                              className="text-xs h-6"
                              data-tour-id={isFirstRow ? "evidence-expander" : undefined}
                            >
                              {isExpanded ? (
                                <>
                                  <ChevronUp className="w-3 h-3 mr-1" />
                                  Collapse
                                </>
                              ) : (
                                <>
                                  <ChevronDown className="w-3 h-3 mr-1" />
                                  {row.evidence.length - 1} more
                                </>
                              )}
                            </Button>
                          )}
                          {isExpanded && (
                            <div className="space-y-2 pl-4 border-l-2 border-border">
                              {row.evidence.slice(1).map((ev, idx) => (
                                <div key={idx} className="flex items-start gap-2">
                                  <div className="flex-1">
                                    <p className="text-xs text-muted-foreground">{ev.summary}</p>
                                    <a
                                      href={ev.url}
                                      target="_blank"
                                      rel="noopener noreferrer"
                                      className="text-xs text-primary hover:underline inline-flex items-center gap-1 mt-1"
                                    >
                                      {ev.title}
                                      <ExternalLink className="w-3 h-3" />
                                    </a>
                                  </div>
                                </div>
                              ))}
                            </div>
                          )}
                        </div>
                      ) : (
                        <span className="text-muted-foreground">N/A</span>
                      )}
                    </td>
                    <td className="py-3 px-2 align-top w-[9rem]" data-tour-id={isFirstRow ? "top-k-event" : undefined}>
                      <div className="space-y-2">
                        {isHit ? (
                          <Badge
                            variant="outline"
                            className="border-emerald-200 bg-emerald-50 text-emerald-700"
                            title="Hit: the ranked company matched a tracked 8-K event in the evaluation window."
                          >
                            <CheckCircle2 className="text-emerald-600" />
                            Hit
                          </Badge>
                        ) : (
                          <Badge
                            variant="outline"
                            className="border-rose-200 bg-rose-50 text-rose-700"
                            title="Miss: no tracked 8-K event was found for this ranked company in the evaluation window."
                          >
                            <XCircle className="text-rose-600" />
                            Miss
                          </Badge>
                        )}
                        {row.event ? (
                          <div className="text-xs space-y-1">
                            <div className="flex flex-wrap items-baseline gap-x-2 gap-y-1">
                              <span className="font-mono">{row.event.form}</span>
                              {eventItems.length > 0 && (
                                <span className="text-muted-foreground">Item {eventItems.join(", ")}</span>
                              )}
                            </div>
                            {row.event.filing_date && (
                              <div className="text-muted-foreground">
                                {new Date(row.event.filing_date).toLocaleDateString("en-US")}
                              </div>
                            )}
                          </div>
                        ) : (
                          <span className="text-xs text-muted-foreground">No tracked filing</span>
                        )}
                        {row.event?.url ? (
                          <a
                            href={row.event.url}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="text-xs text-primary hover:underline inline-flex items-center gap-1"
                          >
                            View SEC filing
                            <ExternalLink className="w-3 h-3 shrink-0" />
                          </a>
                        ) : null}
                      </div>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </CardContent>
    </Card>
  )
}
