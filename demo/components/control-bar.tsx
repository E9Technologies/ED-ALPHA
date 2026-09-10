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
    <div className="metric-card rounded-xl p-6 shadow-lg" data-tour-id="experiment-controls">
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
        <div className="w-fit max-w-full space-y-2" data-tour-id="experiment-selector">
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

        <div className="w-fit max-w-full space-y-2" data-tour-id="run-selector">
          <Label htmlFor="run-select" className="text-sm font-semibold text-foreground">
            Model / Run
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
              {selectedExperiment?.runs.map((run) => (
                <SelectItem key={run.id} value={run.id.toString()}>
                  {run.model_label} · Run {run.id}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </div>
      <p className="mt-4 text-sm text-muted-foreground">
        Paper baselines · Company score = average of article scores (1–5).
      </p>
    </div>
  )
}
