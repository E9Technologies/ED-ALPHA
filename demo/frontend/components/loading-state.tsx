import { Loader2 } from "lucide-react"

export function LoadingState() {
  return (
    <div className="flex flex-col items-center justify-center py-12 space-y-4">
      <Loader2 className="w-8 h-8 animate-spin text-primary" />
      <p className="text-muted-foreground">Loading…</p>
    </div>
  )
}
