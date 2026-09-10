"use client"

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import type { RunMetric } from "@/types/api"
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Tooltip,
  type Plugin,
  type ChartOptions,
  type ChartData,
} from "chart.js"
import { Scatter } from "react-chartjs-2"

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Tooltip)

function computeF1(precision: number, recall: number) {
  if (precision + recall === 0) return 0
  return (2 * precision * recall) / (precision + recall)
}

const F1_CONTOUR_LEVELS = [0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]

const F1_BASE_COLOR: [number, number, number] = [220, 38, 38] // warm red tone
const BEST_F1_COLOR = "rgb(190, 18, 60)"

function getPrecisionForF1(f1: number, recall: number) {
  const denominator = 2 * recall - f1
  if (denominator <= 0) return null
  const precision = (f1 * recall) / denominator
  if (precision < 0 || precision > 1) return null
  return precision
}

function getF1Color(f1: number, alpha = 1) {
  const clamped = Math.min(Math.max(f1, 0), 1)
  const soften = 1 - clamped * 0.85
  const [baseR, baseG, baseB] = F1_BASE_COLOR
  const r = Math.round(255 * soften + baseR * (1 - soften))
  const g = Math.round(255 * soften + baseG * (1 - soften))
  const b = Math.round(255 * soften + baseB * (1 - soften))
  return `rgba(${r}, ${g}, ${b}, ${alpha})`
}

const f1ContourPlugin: Plugin<"scatter"> = {
  id: "f1Contours",
  afterDraw: (chart) => {
    const xScale = chart.scales.x
    const yScale = chart.scales.y
    if (!xScale || !yScale) return

    const ctx = chart.ctx
    ctx.save()
    ctx.setLineDash([6, 6])
    ctx.lineWidth = 1.5
    ctx.strokeStyle = "rgba(15, 23, 42, 0.35)"

    F1_CONTOUR_LEVELS.forEach((f1) => {
      ctx.beginPath()
      let hasPoint = false

      for (let recall = 0.02; recall <= 1.0001; recall += 0.02) {
        const precision = getPrecisionForF1(f1, recall)
        if (precision === null) continue

        const x = xScale.getPixelForValue(recall)
        const y = yScale.getPixelForValue(precision)

        if (!hasPoint) {
          ctx.moveTo(x, y)
          hasPoint = true
        } else {
          ctx.lineTo(x, y)
        }
      }

      if (!hasPoint) return

      ctx.stroke()

      const labelRecallCandidates = [0.95, 0.9, 0.85]
      let labelPoint: { x: number; y: number } | null = null
      for (const recall of labelRecallCandidates) {
        const precision = getPrecisionForF1(f1, recall)
        if (precision === null) continue
        labelPoint = {
          x: xScale.getPixelForValue(recall),
          y: yScale.getPixelForValue(precision),
        }
        break
      }

      if (labelPoint) {
        ctx.save()
        ctx.setLineDash([])
        ctx.fillStyle = "rgba(15, 23, 42, 0.65)"
        ctx.font = "10px Inter, sans-serif"
        ctx.fillText(`F1=${(f1 * 100).toFixed(0)}%`, labelPoint.x - 22, labelPoint.y - 4)
        ctx.restore()
      }
    })

    ctx.restore()
  },
}

interface MetricsChartProps {
  metrics: RunMetric[]
  selectedK: number
  onKSelect: (k: number) => void
}

type ScatterPoint = {
  x: number // recall
  y: number // precision
  k: number
  f1: number
  tp: number
}

const formatPercent = (value: number) => `${(value * 100).toFixed(2)}%`

export function MetricsChart({ metrics, selectedK, onKSelect }: MetricsChartProps) {
  const sortedMetrics = [...metrics].sort((a, b) => a.k - b.k)

  const selectedIndex = sortedMetrics.findIndex((m) => m.k === selectedK)

  const enhancedMetrics = sortedMetrics.map((metric, index) => {
    const truePositives = metric.positives_in_top
    return {
      ...metric,
      f1: computeF1(metric.precision, metric.recall),
      truePositives,
    }
  })

  let bestIndex = -1
  enhancedMetrics.forEach((metric, index) => {
    if (bestIndex === -1 || metric.f1 > enhancedMetrics[bestIndex].f1) {
      bestIndex = index
    }
  })
  if (bestIndex === -1 && enhancedMetrics.length > 0) {
    bestIndex = 0
  }

  const scatterPoints: ScatterPoint[] = enhancedMetrics.map((metric) => ({
    x: metric.recall,
    y: metric.precision,
    k: metric.k,
    f1: metric.f1,
    tp: metric.truePositives,
  }))

  const selectedMetric = enhancedMetrics[selectedIndex] ?? enhancedMetrics[0] ?? null
  const selectedMetricRows = [
    ["K", selectedMetric ? selectedMetric.k : "-"],
    ["Precision@K", selectedMetric ? formatPercent(selectedMetric.precision) : "-"],
    ["Recall@K", selectedMetric ? formatPercent(selectedMetric.recall) : "-"],
    ["F1@K", selectedMetric ? formatPercent(selectedMetric.f1) : "-"],
    ["TP@K", selectedMetric ? selectedMetric.truePositives : "-"],
  ]

  const data: ChartData<"scatter", ScatterPoint[]> = {
    datasets: [
      {
        label: "PR@K",
        type: "scatter" as const,
        data: scatterPoints,
        showLine: false,
        parsing: false as const,
        pointBackgroundColor: enhancedMetrics.map((metric, i) =>
          i === bestIndex
            ? BEST_F1_COLOR
            : getF1Color(metric.f1, i === selectedIndex ? 1 : 0.85),
        ),
        pointBorderColor: enhancedMetrics.map((metric, i) => {
          if (i === selectedIndex) return "rgb(255, 255, 255)"
          if (i === bestIndex) return "rgba(185, 28, 28, 0.9)"
          return getF1Color(metric.f1, 0.4)
        }),
        pointBorderWidth: 0,
      },
    ],
  }

  const options: ChartOptions<"scatter"> = {
    responsive: true,
    maintainAspectRatio: false,
    elements: {
      point: {
        radius: 6,
        hoverRadius: 6,
        hitRadius: 6,
      },
    },
    animation: false,
    transitions: {
      active: {
        animation: {
          duration: 0,
        },
      },
    },
    interaction: {
      mode: "nearest",
      intersect: true,
    },
    plugins: {
      legend: {
        display: false,
      },
      tooltip: {
        displayColors: false,
        callbacks: {
          title: (contexts) => {
            const context = contexts[0]
            const raw = context?.raw as ScatterPoint | undefined
            return raw ? [`K=${raw.k}`] : []
          },
          label: (context) => {
            const raw = context.raw as ScatterPoint
            return [
              `Precision@K: ${(raw.y * 100).toFixed(2)}%`,
              `Recall@K: ${(raw.x * 100).toFixed(2)}%`,
              `F1@K: ${(raw.f1 * 100).toFixed(2)}%`,
              `TP@K: ${raw.tp}`,
            ]
          },
        },
      },
    },
    scales: {
      x: {
        type: "linear",
        title: {
          display: true,
          text: "Recall@K",
          color: "rgb(113, 113, 122)",
        },
        ticks: {
          color: "rgb(113, 113, 122)",
          callback: (value) => `${(Number(value) * 100).toFixed(0)}%`,
        },
        grid: {
          color: "rgba(228, 228, 231, 0.5)",
        },
        min: 0,
        max: 1,
      },
      y: {
        title: {
          display: true,
          text: "Precision@K",
          color: "rgb(113, 113, 122)",
        },
        ticks: {
          color: "rgb(113, 113, 122)",
          callback: (value) => `${(Number(value) * 100).toFixed(0)}%`,
        },
        grid: {
          color: "rgba(228, 228, 231, 0.5)",
        },
        min: 0,
        max: 1,
      },
    },
    onClick: (_event, elements) => {
      if (elements.length > 0) {
        const index = elements[0].index
        const point = scatterPoints[index]
        if (point) {
          onKSelect(point.k)
        }
      }
    },
  }

  return (
    <Card className="w-full" data-tour-id="metrics-viewer">
      <CardHeader>
        <CardTitle className="text-xl">Metrics (Recall@K / Precision@K / F1@K / TP@K )</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="mb-4 flex items-center gap-2" role="group" aria-label="Select K">
          <span className="mr-1 text-sm font-medium">Top-K</span>
          {sortedMetrics.map((metric) => (
            <Button key={metric.k} size="sm" variant={metric.k === selectedK ? "default" : "outline"}
              aria-pressed={metric.k === selectedK} onClick={() => onKSelect(metric.k)}>
              K={metric.k}
            </Button>
          ))}
        </div>
        <div className="flex flex-col items-start gap-4 sm:flex-row">
          <div className="w-full min-w-0 flex-1">
            <div className="h-[420px] w-full">
              <Scatter data={data} options={options} plugins={[f1ContourPlugin]} />
            </div>
          </div>
          <div className="w-[clamp(170px,25vw,240px)] shrink-0" data-tour-id="selected-k-summary">
            <div className="overflow-hidden rounded-lg border border-border">
              <table className="w-full table-fixed text-xs">
                <colgroup>
                  <col />
                  <col className="w-[4.25rem]" />
                </colgroup>
                <thead className="bg-muted/60 text-left uppercase tracking-wide text-muted-foreground">
                  <tr>
                    <th className="px-2.5 py-2 font-semibold">Metric</th>
                    <th className="px-2.5 py-2 text-right font-semibold">Value</th>
                  </tr>
                </thead>
                <tbody>
                  {selectedMetricRows.map(([label, value], index) => (
                    <tr key={label} className="border-t border-border">
                      <td className="truncate px-2.5 py-2 text-muted-foreground">{label}</td>
                      <td className={`px-2.5 py-2 text-right tabular-nums ${index === 0 ? "font-semibold" : ""}`}>
                        {value}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
        <p className="mt-4 text-center text-sm text-muted-foreground">
          Select K above or click a point on the chart (Current: K={selectedK})
        </p>
      </CardContent>
    </Card>
  )
}
