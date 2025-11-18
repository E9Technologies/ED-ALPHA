"use client"

import { Button } from "@/components/ui/button"
import { AlertCircle } from "lucide-react"

interface ErrorStateProps {
  message: string
  onRetry: () => void
}

export function ErrorState({ message, onRetry }: ErrorStateProps) {
  return (
    <div className="flex flex-col items-center justify-center py-12 space-y-4">
      <AlertCircle className="w-12 h-12 text-destructive" />
      <p className="text-destructive font-medium">{message}</p>
      <Button onClick={onRetry} variant="outline">
        Retry
      </Button>
    </div>
  )
}
