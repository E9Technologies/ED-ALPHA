import { FileQuestion } from "lucide-react"

interface EmptyStateProps {
  message: string
}

export function EmptyState({ message }: EmptyStateProps) {
  return (
    <div className="flex flex-col items-center justify-center py-12 space-y-4">
      <FileQuestion className="w-12 h-12 text-muted-foreground" />
      <p className="text-muted-foreground">{message}</p>
    </div>
  )
}
