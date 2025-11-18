"use client"

import { useState } from "react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { ChevronDown, ChevronUp, Download, ExternalLink } from "lucide-react"
import type { EventItems, Experiment, ResultsResponse } from "@/types/api"

interface ResultsTableProps {
  results: ResultsResponse
  experiment: Experiment | null
}

export function ResultsTable({ results, experiment }: ResultsTableProps) {
  const [expandedRows, setExpandedRows] = useState<Set<number>>(new Set())

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
    <Card>
      <CardHeader className="flex flex-row items-center justify-between">
        <CardTitle className="text-xl">Top-{results.k} Companies</CardTitle>
        <Button variant="outline" size="sm" onClick={downloadJson} disabled={!experiment}>
          <Download className="w-4 h-4 mr-2" />
          Download JSON
        </Button>
      </CardHeader>
      <CardContent>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-border">
                <th className="text-left py-3 px-4 font-medium text-muted-foreground">CIK</th>
                <th className="text-left py-3 px-4 font-medium text-muted-foreground">Company Name</th>
                <th className="text-right py-3 px-4 font-medium text-muted-foreground">Score</th>
                <th className="text-left py-3 px-4 font-medium text-muted-foreground">Evidence</th>
                <th className="text-left py-3 px-4 font-medium text-muted-foreground">Event</th>
                <th className="text-left py-3 px-4 font-medium text-muted-foreground">Link</th>
              </tr>
            </thead>
            <tbody>
              {results.results.map((row) => {
                const isExpanded = expandedRows.has(row.cik)
                const firstEvidence = row.evidence[0]
                const hasMoreEvidence = row.evidence.length > 1
                const eventItems = normalizeEventItems(row.event?.items)

                return (
                  <tr key={row.cik} className="border-b border-border hover:bg-secondary/50">
                    <td className="py-3 px-4 font-mono">{row.cik}</td>
                    <td className="py-3 px-4">{row.company_name || "—"}</td>
                    <td className="py-3 px-4 text-right font-mono font-semibold">{Math.round(row.total_score)}</td>
                    <td className="py-3 px-4">
                      {firstEvidence ? (
                        <div className="space-y-2">
                          <div className="flex items-start gap-2">
                            <div className="flex-1">
                              <p className="text-xs text-muted-foreground line-clamp-2">{firstEvidence.summary}</p>
                              <a
                                href={firstEvidence.url}
                                target="_blank"
                                rel="noopener noreferrer"
                                className="text-xs text-primary hover:underline inline-flex items-center gap-1 mt-1"
                              >
                                {firstEvidence.title}
                                <ExternalLink className="w-3 h-3" />
                              </a>
                            </div>
                          </div>
                          {hasMoreEvidence && (
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => toggleRow(row.cik)}
                              className="text-xs h-6"
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
                        <span className="text-muted-foreground">—</span>
                      )}
                    </td>
                    <td className="py-3 px-4">
                      {row.event ? (
                        <div className="text-xs space-y-1">
                          <div className="font-mono">{row.event.form}</div>
                          {eventItems.length > 0 && (
                            <ul className="space-y-1 text-muted-foreground">
                              {eventItems.map((item, idx) => (
                                <li key={idx} className="flex gap-2">
                                  <span className="font-mono text-[0.65rem] text-muted-foreground/80">•</span>
                                  <span>{item}</span>
                                </li>
                              ))}
                            </ul>
                          )}
                          {row.event.filing_date && (
                            <div className="text-muted-foreground">
                              {new Date(row.event.filing_date).toLocaleDateString("en-US")}
                            </div>
                          )}
                        </div>
                      ) : (
                        <span className="text-muted-foreground">—</span>
                      )}
                    </td>
                    <td className="py-3 px-4">
                      {row.event?.url ? (
                        <a
                          href={row.event.url}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-primary hover:underline inline-flex items-center gap-1"
                        >
                          <ExternalLink className="w-4 h-4" />
                        </a>
                      ) : (
                        <span className="text-muted-foreground">—</span>
                      )}
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
