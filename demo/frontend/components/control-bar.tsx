"use client"

import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import type { Experiment } from "@/types/api"

interface ControlBarProps {
  experiments: Experiment[]
  selectedExperiment: Experiment | null
  selectedRunId: number | null
  onExperimentChange: (experiment: Experiment) => void
  onRunChange: (runId: number) => void
}

export function ControlBar({
  experiments,
  selectedExperiment,
  selectedRunId,
  onExperimentChange,
  onRunChange,
}: ControlBarProps) {
  return (
    <div className="metric-card rounded-xl p-6 shadow-lg">
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
        <div className="space-y-2">
          <Label htmlFor="experiment-select" className="text-sm font-semibold text-foreground">
            Experiment
          </Label>
          <Select
            value={selectedExperiment?.id.toString()}
            onValueChange={(value) => {
              const exp = experiments.find((e) => e.id === Number.parseInt(value))
              if (exp) {
                onExperimentChange(exp)
                if (exp.run_ids.length > 0) {
                  onRunChange(exp.run_ids[exp.run_ids.length - 1])
                }
              }
            }}
          >
            <SelectTrigger
              id="experiment-select"
              className="bg-secondary border-border hover:border-primary transition-colors"
            >
              <SelectValue placeholder="Select Experiment" />
            </SelectTrigger>
            <SelectContent>
              {experiments.map((exp) => (
                <SelectItem key={exp.id} value={exp.id.toString()}>
                  Experiment {exp.id}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        <div className="space-y-2">
          <Label htmlFor="run-select" className="text-sm font-semibold text-foreground">
            Run
          </Label>
          <Select
            value={selectedRunId?.toString()}
            onValueChange={(value) => onRunChange(Number.parseInt(value))}
            disabled={!selectedExperiment}
          >
            <SelectTrigger
              id="run-select"
              className="bg-secondary border-border hover:border-primary transition-colors"
            >
              <SelectValue placeholder="Select Run" />
            </SelectTrigger>
            <SelectContent>
              {selectedExperiment?.run_ids.map((runId) => (
                <SelectItem key={runId} value={runId.toString()}>
                  Run {runId}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </div>
    </div>
  )
}
