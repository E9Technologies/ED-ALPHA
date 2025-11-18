"use client"

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import type { Experiment } from "@/types/api"

interface ExperimentCardProps {
  experiment: Experiment
}

export function ExperimentCard({ experiment }: ExperimentCardProps) {
  return (
    <Card className="metric-card shadow-lg border-border">
      <CardHeader className="border-b border-border/50">
        <CardTitle className="text-xl font-bold">
          Experiment ID: <span className="gold-accent">{experiment.id}</span>
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4 pt-6">
        <p className="text-sm text-muted-foreground">
          Parameters below summarize the configuration used when generating this experiment.
        </p>
        <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
          <div className="stat-highlight p-3 rounded-lg">
            <div className="text-xs text-muted-foreground mb-1">Predict Date</div>
            <div className="font-mono text-sm font-semibold">
              {new Date(experiment.predict_date).toLocaleDateString("en-US")}
            </div>
          </div>
          <div className="stat-highlight p-3 rounded-lg">
            <div className="text-xs text-muted-foreground mb-1">Horizon</div>
            <div className="font-mono text-sm font-semibold">
              {experiment.horizon_days} <span className="text-muted-foreground">days</span>
            </div>
          </div>
          <div className="stat-highlight p-3 rounded-lg">
            <div className="text-xs text-muted-foreground mb-1">Seed</div>
            <div className="font-mono text-sm font-semibold">{experiment.seed}</div>
          </div>
          <div className="stat-highlight p-3 rounded-lg">
            <div className="text-xs text-muted-foreground mb-1">Neg Multiplier</div>
            <div className="font-mono text-sm font-semibold">
              {experiment.neg_multiplier}
              <span className="text-muted-foreground">x</span>
            </div>
          </div>
          <div className="col-span-2 stat-highlight p-3 rounded-lg">
            <div className="text-xs text-muted-foreground mb-1">Item Codes</div>
            <div className="font-mono text-xs">{experiment.item_codes.join(", ")}</div>
          </div>
        </div>
      </CardContent>
    </Card>
  )
}
