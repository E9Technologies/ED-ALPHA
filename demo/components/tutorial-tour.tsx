"use client"

import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import type { CSSProperties, PointerEvent as ReactPointerEvent } from "react"
import { ArrowLeft, ArrowRight, Check, X } from "lucide-react"
import { Button } from "@/components/ui/button"

type TutorialStep = {
  id: string
  targetId: string
  title: string
  description: string[]
  bullets?: string[]
}

const TUTORIAL_STEPS: TutorialStep[] = [
  {
    id: "welcome",
    targetId: "product-identity",
    title: "Welcome to ED-ALPHA",
    description: [
      "This demo benchmarks predictions of investment-relevant corporate events using public data.",
      "The September 2025 snapshot uses GDELT news from Aug 2-27 to predict Form 8-K events filed from Sep 1-5.",
    ],
  },
  {
    id: "architecture",
    targetId: "benchmark-overview",
    title: "Benchmark overview",
    description: [
      "Public news in, ranked companies out, SEC Form 8-K labels and Top-K metrics for evaluation.",
    ],
  },
  {
    id: "experiment-selection",
    targetId: "experiment-selector",
    title: "Select an experiment",
    description: [
      "An experiment defines the benchmark scenario. It fixes the prediction date, event horizon, tracked 8-K item codes, random seed, company sampling, and other setup parameters.",
      "Changing the experiment changes the task being evaluated, so metrics and rankings should only be compared within the same experiment unless the setup is intentionally different.",
    ],
  },
  {
    id: "run-selection",
    targetId: "run-selector",
    title: "Select a run",
    description: [
      "A run is one model or scoring execution inside the selected experiment. It contains that method's scores, Top-K ranking, news signals, and evaluation metrics.",
      "These three runs reproduce the paper's baselines. Each company is ranked by the average of its article scores, using the same fixed benchmark setup.",
    ],
  },
  {
    id: "configuration",
    targetId: "experiment-config",
    title: "Read the benchmark setup",
    description: [
      "This card records the parameters needed to reproduce the snapshot: prediction date, horizon, seed, negative multiplier, and tracked SEC item codes.",
    ],
  },
  {
    id: "metrics",
    targetId: "metrics-viewer",
    title: "Compare ranking quality",
    description: [
      "The chart plots Precision@K and Recall@K, with F1 contours showing the balance between the two.",
      "Select K=5, 25 or 50 using the buttons or chart points to refresh the company ranking below.",
    ],
  },
  {
    id: "top-k-ranking",
    targetId: "top-k-first-row",
    title: "Audit the Top-K ranking",
    description: [
      "Start with the first row: it is the model's highest-ranked prediction for the selected K.",
      "Avg. score is the mean article score for the company. News Signals shows its article evidence, Outcome shows the benchmark label, and Event links to the tracked Form 8-K filing behind a hit.",
    ],
  },
]

interface TutorialTourProps {
  open: boolean
  onClose: () => void
  onStepChange?: (stepId: string | null) => void
}

type ViewportSize = {
  width: number
  height: number
}

type PanelSize = {
  width: number
  height: number
}

type TargetRect = {
  top: number
  left: number
  right: number
  bottom: number
  width: number
  height: number
}

type DragPosition = {
  left: number
  top: number
  width: number
}

const SPOTLIGHT_PADDING = 10
const PANEL_WIDTH = 360
const PANEL_MARGIN = 16
const PANEL_ESTIMATED_HEIGHT = 440

function clamp(value: number, min: number, max: number) {
  return Math.min(Math.max(value, min), max)
}

function getTargetElement(targetId: string) {
  if (typeof document === "undefined") {
    return null
  }
  return document.querySelector<HTMLElement>(`[data-tour-id="${targetId}"]`)
}

function stepHasTarget(step: TutorialStep) {
  return Boolean(getTargetElement(step.targetId))
}

function getMeasuredTargetRect(element: HTMLElement): TargetRect {
  return element.getBoundingClientRect()
}

export function TutorialTour({ open, onClose, onStepChange }: TutorialTourProps) {
  const [stepIndex, setStepIndex] = useState(0)
  const [targetRect, setTargetRect] = useState<TargetRect | null>(null)
  const [viewport, setViewport] = useState<ViewportSize>({ width: 1024, height: 768 })
  const [panelSize, setPanelSize] = useState<PanelSize>({ width: PANEL_WIDTH, height: PANEL_ESTIMATED_HEIGHT })
  const [dragPosition, setDragPosition] = useState<DragPosition | null>(null)
  const panelRef = useRef<HTMLElement | null>(null)
  const dragState = useRef({
    offsetX: 0,
    offsetY: 0,
    width: PANEL_WIDTH,
    height: PANEL_ESTIMATED_HEIGHT,
  })
  const allowProgrammaticScrollUntil = useRef(0)
  const lockedScrollPosition = useRef({ x: 0, y: 0 })

  const currentStep = TUTORIAL_STEPS[stepIndex] ?? TUTORIAL_STEPS[0]
  const isMobile = viewport.width < 768

  const closeTour = useCallback(() => {
    onClose()
  }, [onClose])

  const findVisibleStep = useCallback((startIndex: number, direction: 1 | -1) => {
    for (let index = startIndex; index >= 0 && index < TUTORIAL_STEPS.length; index += direction) {
      if (stepHasTarget(TUTORIAL_STEPS[index])) {
        return index
      }
    }
    return null
  }, [])

  const measureTarget = useCallback(() => {
    if (!open) {
      return
    }

    setViewport({
      width: window.innerWidth,
      height: window.innerHeight,
    })

    const target = getTargetElement(currentStep.targetId)
    if (!target) {
      const nextIndex = findVisibleStep(stepIndex + 1, 1) ?? findVisibleStep(stepIndex - 1, -1)
      if (nextIndex !== null) {
        setStepIndex(nextIndex)
        return
      }
      closeTour()
      return
    }

    setTargetRect(getMeasuredTargetRect(target))
  }, [closeTour, currentStep, findVisibleStep, open, stepIndex])

  useEffect(() => {
    if (!open) {
      onStepChange?.(null)
      setDragPosition(null)
      return
    }

    const firstVisibleIndex = findVisibleStep(0, 1)
    setStepIndex(firstVisibleIndex ?? 0)
  }, [findVisibleStep, onStepChange, open])

  useEffect(() => {
    setDragPosition(null)
  }, [currentStep.id])

  useEffect(() => {
    if (!open) {
      return
    }

    onStepChange?.(currentStep.id)

    const target = getTargetElement(currentStep.targetId)
    if (!target) {
      const nextIndex = findVisibleStep(stepIndex + 1, 1) ?? findVisibleStep(stepIndex - 1, -1)
      if (nextIndex !== null && nextIndex !== stepIndex) {
        setStepIndex(nextIndex)
      }
      return
    }

    allowProgrammaticScrollUntil.current = Date.now() + 350
    target.scrollIntoView({
      block: "center",
      inline: "nearest",
      behavior: "auto",
    })
    const animationFrame = window.requestAnimationFrame(measureTarget)
    const timeout = window.setTimeout(() => {
      measureTarget()
      lockedScrollPosition.current = {
        x: window.scrollX,
        y: window.scrollY,
      }
    }, 280)

    window.addEventListener("resize", measureTarget)
    window.addEventListener("scroll", measureTarget, true)

    return () => {
      window.cancelAnimationFrame(animationFrame)
      window.clearTimeout(timeout)
      window.removeEventListener("resize", measureTarget)
      window.removeEventListener("scroll", measureTarget, true)
    }
  }, [currentStep.id, currentStep.targetId, findVisibleStep, measureTarget, onStepChange, open, stepIndex])

  useEffect(() => {
    if (!open) {
      return
    }

    lockedScrollPosition.current = {
      x: window.scrollX,
      y: window.scrollY,
    }

    let restoreFrame = 0
    const restoreScrollPosition = () => {
      if (Date.now() < allowProgrammaticScrollUntil.current) {
        return
      }

      window.cancelAnimationFrame(restoreFrame)
      restoreFrame = window.requestAnimationFrame(() => {
        allowProgrammaticScrollUntil.current = Date.now() + 80
        window.scrollTo(lockedScrollPosition.current.x, lockedScrollPosition.current.y)
      })
    }
    const restoreInterval = window.setInterval(() => {
      if (Date.now() < allowProgrammaticScrollUntil.current) {
        return
      }
      if (window.scrollX !== lockedScrollPosition.current.x || window.scrollY !== lockedScrollPosition.current.y) {
        restoreScrollPosition()
      }
    }, 80)

    window.addEventListener("scroll", restoreScrollPosition, true)
    document.addEventListener("scroll", restoreScrollPosition, true)
    return () => {
      window.clearInterval(restoreInterval)
      window.cancelAnimationFrame(restoreFrame)
      window.removeEventListener("scroll", restoreScrollPosition, true)
      document.removeEventListener("scroll", restoreScrollPosition, true)
    }
  }, [open])

  useEffect(() => {
    if (!open) {
      return
    }

    const scrollKeys = new Set(["ArrowUp", "ArrowDown", "PageUp", "PageDown", "Home", "End", " "])
    const isInsideTourPanel = (target: EventTarget | null) =>
      target instanceof HTMLElement && Boolean(target.closest("[data-tour-panel]"))
    const preventBackgroundScroll = (event: WheelEvent | TouchEvent) => {
      if (isInsideTourPanel(event.target)) {
        return
      }
      event.preventDefault()
    }
    const handleKeyDown = (event: KeyboardEvent) => {
      if (scrollKeys.has(event.key) && !isInsideTourPanel(event.target)) {
        event.preventDefault()
      }
      if (event.key === "Escape") {
        closeTour()
      }
      if (event.key === "ArrowRight") {
        event.preventDefault()
        const nextIndex = findVisibleStep(stepIndex + 1, 1)
        if (nextIndex === null) {
          closeTour()
        } else {
          setStepIndex(nextIndex)
        }
      }
      if (event.key === "ArrowLeft") {
        event.preventDefault()
        const previousIndex = findVisibleStep(stepIndex - 1, -1)
        if (previousIndex !== null) {
          setStepIndex(previousIndex)
        }
      }
    }

    window.addEventListener("keydown", handleKeyDown)
    window.addEventListener("wheel", preventBackgroundScroll, { passive: false, capture: true })
    window.addEventListener("touchmove", preventBackgroundScroll, { passive: false, capture: true })
    return () => {
      window.removeEventListener("keydown", handleKeyDown)
      window.removeEventListener("wheel", preventBackgroundScroll, { capture: true })
      window.removeEventListener("touchmove", preventBackgroundScroll, { capture: true })
    }
  }, [closeTour, findVisibleStep, open, stepIndex])

  useEffect(() => {
    if (!open || !panelRef.current) {
      return
    }

    const panel = panelRef.current
    const updatePanelSize = () => {
      const rect = panel.getBoundingClientRect()
      setPanelSize((currentSize) => {
        if (currentSize.width === rect.width && currentSize.height === rect.height) {
          return currentSize
        }

        return {
          width: rect.width,
          height: rect.height,
        }
      })
    }

    updatePanelSize()
    const observer = new ResizeObserver(updatePanelSize)
    observer.observe(panel)

    return () => {
      observer.disconnect()
    }
  }, [currentStep.id, open])

  useEffect(() => {
    if (!dragPosition) {
      return
    }

    setDragPosition((currentPosition) => {
      if (!currentPosition) {
        return currentPosition
      }

      const width = panelSize.width || currentPosition.width
      const height = panelSize.height || dragState.current.height

      const nextLeft = clamp(
        currentPosition.left,
        PANEL_MARGIN,
        Math.max(PANEL_MARGIN, viewport.width - width - PANEL_MARGIN),
      )
      const nextTop = clamp(
        currentPosition.top,
        PANEL_MARGIN,
        Math.max(PANEL_MARGIN, viewport.height - height - PANEL_MARGIN),
      )

      if (nextLeft === currentPosition.left && nextTop === currentPosition.top) {
        return currentPosition
      }

      return {
        ...currentPosition,
        left: nextLeft,
        top: nextTop,
      }
    })
  }, [dragPosition, panelSize.height, panelSize.width, viewport.height, viewport.width])

  const spotlightStyle = useMemo<CSSProperties>(() => {
    if (!targetRect) {
      return {}
    }

    const top = Math.max(PANEL_MARGIN, targetRect.top - SPOTLIGHT_PADDING)
    const left = Math.max(PANEL_MARGIN, targetRect.left - SPOTLIGHT_PADDING)
    const maxWidth = viewport.width - left - PANEL_MARGIN
    const maxHeight = viewport.height - top - PANEL_MARGIN

    return {
      top,
      left,
      width: Math.min(targetRect.width + SPOTLIGHT_PADDING * 2, maxWidth),
      height: Math.min(targetRect.height + SPOTLIGHT_PADDING * 2, maxHeight),
      boxShadow: "0 0 0 9999px rgba(15, 23, 42, 0.62)",
    }
  }, [targetRect, viewport.height, viewport.width])

  const anchoredPanelStyle = useMemo<CSSProperties>(() => {
    const targetFillsViewport =
      targetRect &&
      (targetRect.height > viewport.height * 0.72 || targetRect.width > viewport.width * 0.86)

    if (isMobile) {
      if (targetFillsViewport) {
        return {
          left: PANEL_MARGIN,
          right: PANEL_MARGIN,
          top: PANEL_MARGIN,
        }
      }

      return {
        left: PANEL_MARGIN,
        right: PANEL_MARGIN,
        bottom: PANEL_MARGIN,
      }
    }

    if (!targetRect) {
      return {
        left: "50%",
        top: "50%",
        transform: "translate(-50%, -50%)",
        width: PANEL_WIDTH,
      }
    }

    if (targetFillsViewport) {
      return {
        left: Math.max(PANEL_MARGIN, viewport.width - PANEL_WIDTH - PANEL_MARGIN),
        top: PANEL_MARGIN,
        width: PANEL_WIDTH,
      }
    }

    const panelWidth = panelSize.width || PANEL_WIDTH
    const panelHeight = panelSize.height || PANEL_ESTIMATED_HEIGHT
    const maxLeft = Math.max(PANEL_MARGIN, viewport.width - panelWidth - PANEL_MARGIN)
    const maxTop = Math.max(PANEL_MARGIN, viewport.height - panelHeight - PANEL_MARGIN)
    const fitsRight = targetRect.right + PANEL_MARGIN + panelWidth <= viewport.width
    const fitsLeft = targetRect.left - PANEL_MARGIN - panelWidth >= 0
    const top = clamp(targetRect.top, PANEL_MARGIN, maxTop)

    if (fitsRight) {
      return {
        left: targetRect.right + PANEL_MARGIN,
        top,
        width: PANEL_WIDTH,
      }
    }

    if (fitsLeft) {
      return {
        left: targetRect.left - PANEL_MARGIN - panelWidth,
        top,
        width: PANEL_WIDTH,
      }
    }

    return {
      left: clamp(targetRect.left, PANEL_MARGIN, maxLeft),
      top: clamp(targetRect.bottom + PANEL_MARGIN, PANEL_MARGIN, maxTop),
      width: PANEL_WIDTH,
    }
  }, [isMobile, panelSize.height, panelSize.width, targetRect, viewport.height, viewport.width])

  const panelStyle = useMemo<CSSProperties>(() => {
    if (!dragPosition) {
      return anchoredPanelStyle
    }

    return {
      left: dragPosition.left,
      top: dragPosition.top,
      width: dragPosition.width,
    }
  }, [anchoredPanelStyle, dragPosition])

  if (!open) {
    return null
  }

  const previousIndex = findVisibleStep(stepIndex - 1, -1)
  const nextIndex = findVisibleStep(stepIndex + 1, 1)
  const isLastStep = nextIndex === null

  const startPanelDrag = (event: ReactPointerEvent<HTMLElement>) => {
    if (event.button !== 0) {
      return
    }

    const panel = panelRef.current
    if (!panel) {
      return
    }

    const rect = panel.getBoundingClientRect()
    dragState.current = {
      offsetX: event.clientX - rect.left,
      offsetY: event.clientY - rect.top,
      width: rect.width,
      height: rect.height,
    }
    setDragPosition({
      left: rect.left,
      top: rect.top,
      width: rect.width,
    })
    event.preventDefault()

    const movePanel = (moveEvent: PointerEvent) => {
      const { offsetX, offsetY, width, height } = dragState.current
      setDragPosition({
        left: clamp(moveEvent.clientX - offsetX, PANEL_MARGIN, Math.max(PANEL_MARGIN, window.innerWidth - width - PANEL_MARGIN)),
        top: clamp(moveEvent.clientY - offsetY, PANEL_MARGIN, Math.max(PANEL_MARGIN, window.innerHeight - height - PANEL_MARGIN)),
        width,
      })
    }

    const stopPanelDrag = () => {
      window.removeEventListener("pointermove", movePanel)
      window.removeEventListener("pointerup", stopPanelDrag)
      window.removeEventListener("pointercancel", stopPanelDrag)
    }

    window.addEventListener("pointermove", movePanel)
    window.addEventListener("pointerup", stopPanelDrag)
    window.addEventListener("pointercancel", stopPanelDrag)
  }

  return (
    <div className="fixed inset-0 z-50 pointer-events-none" aria-live="polite">
      <div className="fixed inset-0 pointer-events-auto" aria-hidden="true" />
      <div
        className="pointer-events-none fixed rounded-xl border-2 border-primary bg-transparent transition-all duration-200"
        style={spotlightStyle}
        aria-hidden="true"
      />
      <section
        ref={panelRef}
        data-tour-panel
        role="dialog"
        aria-modal="true"
        aria-labelledby="tutorial-title"
        className="fixed pointer-events-auto flex max-h-[min(38vh,20rem)] flex-col overflow-hidden rounded-lg border bg-popover p-3 text-popover-foreground shadow-2xl md:max-h-[calc(100vh-2rem)] md:p-5"
        style={panelStyle}
      >
        <div
          className="-mx-3 -mt-3 mb-2 flex h-6 cursor-grab touch-none select-none items-center justify-center rounded-t-lg border-b border-border/60 bg-secondary/40 active:cursor-grabbing md:-mx-5 md:-mt-5 md:mb-4 md:h-8"
          onPointerDown={startPanelDrag}
          title="Move panel"
          aria-label="Move tutorial panel"
          role="button"
          tabIndex={0}
        >
          <div className="grid grid-cols-3 gap-1" aria-hidden="true">
            {Array.from({ length: 6 }).map((_, index) => (
              <span key={index} className="h-1.5 w-1.5 rounded-full bg-muted-foreground/45" />
            ))}
          </div>
        </div>
        <div className="mb-2 flex shrink-0 items-start justify-between gap-3 md:mb-4">
          <div className="min-w-0 flex-1">
            <div className="mb-1 text-[10px] font-medium uppercase tracking-wide text-muted-foreground md:mb-2 md:text-xs">
              Step {stepIndex + 1} / {TUTORIAL_STEPS.length}
            </div>
            <h2 id="tutorial-title" className="text-sm font-semibold leading-tight md:text-lg">
              {currentStep.title}
            </h2>
          </div>
          <Button variant="ghost" size="icon-sm" onClick={closeTour} aria-label="Skip tutorial">
            <X className="w-4 h-4" />
          </Button>
        </div>

        <div className="min-h-0 flex-1 space-y-1.5 overflow-y-auto pr-1 text-xs leading-5 text-muted-foreground md:space-y-3 md:text-sm md:leading-6">
          {currentStep.description.map((paragraph) => (
            <p key={paragraph}>{paragraph}</p>
          ))}
          {currentStep.bullets && (
            <ul className="space-y-1.5 md:space-y-2">
              {currentStep.bullets.map((bullet) => (
                <li key={bullet} className="flex gap-2">
                  <span className="mt-[0.45rem] h-1.5 w-1.5 shrink-0 rounded-full bg-primary md:mt-2" />
                  <span>{bullet}</span>
                </li>
              ))}
            </ul>
          )}
        </div>

        <div className="mt-3 flex shrink-0 items-center justify-between gap-2 md:mt-5 md:gap-3">
          <Button variant="ghost" size="sm" onClick={closeTour}>
            Skip
          </Button>
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              className="enabled:cursor-pointer disabled:cursor-not-allowed"
              onClick={() => {
                if (previousIndex !== null) {
                  setStepIndex(previousIndex)
                }
              }}
              disabled={previousIndex === null}
            >
              <ArrowLeft className="w-4 h-4" />
              Back
            </Button>
            <Button
              size="sm"
              className="cursor-pointer"
              onClick={() => {
                if (nextIndex === null) {
                  closeTour()
                } else {
                  setStepIndex(nextIndex)
                }
              }}
            >
              {isLastStep ? (
                <>
                  <Check className="w-4 h-4" />
                  Finish
                </>
              ) : (
                <>
                  Next
                  <ArrowRight className="w-4 h-4" />
                </>
              )}
            </Button>
          </div>
        </div>
      </section>
    </div>
  )
}
