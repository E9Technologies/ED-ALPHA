"use client"

import { useCallback, useEffect, useState } from "react"
import { Activity } from "lucide-react"

interface HeaderProps {
  apiBase: string
}

export function Header({ apiBase }: HeaderProps) {
  const [healthError, setHealthError] = useState<string | null>(null)

  const checkHealth = useCallback(async () => {
    try {
      const response = await fetch(`${apiBase}/health`, { cache: "no-store" })
      if (response.ok) {
        setHealthError(null)
      } else {
        setHealthError("Cannot reach backend health endpoint.")
      }
    } catch (error) {
      console.error("Failed to reach backend health endpoint", error)
      setHealthError("Cannot reach backend health endpoint.")
    }
  }, [apiBase])

  useEffect(() => {
    void checkHealth()
  }, [checkHealth])

  return (
    <header className="border-b border-border bg-gradient-to-r from-card via-card to-secondary/50 backdrop-blur-sm">
      <div className="container mx-auto px-6 py-5 flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-lg bg-gradient-to-br from-primary to-primary/60 flex items-center justify-center">
            <Activity className="w-6 h-6 text-primary-foreground" />
          </div>
          <div>
            <h1 className="text-2xl font-bold text-foreground tracking-tight">ED-ALPHA</h1>
            <p className="text-xs text-muted-foreground font-mono">Event Driven Strategy Analytics Platform</p>
          </div>
        </div>
        {healthError && (
          <div className="bg-destructive/10 text-destructive border border-destructive/30 rounded-md px-3 py-2 text-sm">
            {healthError}
          </div>
        )}
      </div>
    </header>
  )
}
