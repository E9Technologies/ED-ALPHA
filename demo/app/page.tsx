import type { Metadata } from "next"
import Link from "next/link"
import { Activity, ArrowRight, ExternalLink, MousePointerClick, PlayCircle } from "lucide-react"

const BASE_PATH = process.env.NEXT_PUBLIC_BASE_PATH ?? ""
const GITHUB_URL = "https://github.com/E9Technologies/ED-ALPHA"
const VIDEO_SRC = `${BASE_PATH}/videos/ed-alpha-demo-1280x720.mp4`
const GITHUB_ICON_SRC = `${BASE_PATH}/demo_walkthrough/github.png`

export const metadata: Metadata = {
  title: "ED-Alpha",
  description:
    "Event-Driven ALPHA (ED-Alpha) is an open benchmark for ranking companies by future investment-relevant SEC Form 8-K events using public pre-event information.",
}

export default function Page() {
  return (
    <main className="relative isolate min-h-screen overflow-hidden bg-[linear-gradient(180deg,oklch(0.995_0_0)_0%,oklch(0.985_0.012_250)_34%,oklch(0.998_0_0)_68%,oklch(0.985_0.018_85)_100%)] text-foreground">
      <div
        className="pointer-events-none absolute inset-x-0 top-0 -z-10 h-[620px] bg-[linear-gradient(115deg,oklch(0.97_0.018_250_/_0.92)_0%,oklch(1_0_0_/_0.9)_48%,oklch(0.97_0.025_85_/_0.7)_100%)]"
        aria-hidden="true"
      />
      <div
        className="pointer-events-none absolute inset-x-0 top-[520px] -z-10 h-56 bg-[linear-gradient(180deg,oklch(1_0_0_/_0)_0%,oklch(1_0_0_/_0.92)_78%)]"
        aria-hidden="true"
      />
      <header className="mx-auto flex w-full max-w-7xl items-center justify-center px-5 py-5 sm:justify-between sm:px-8">
        <Link href="/" className="flex items-center gap-3" aria-label="ED-Alpha home">
          <span className="flex size-10 items-center justify-center rounded-lg bg-gradient-to-br from-primary to-primary/60">
            <Activity className="size-6 text-primary-foreground" />
          </span>
          <span className="text-sm font-semibold text-slate-950">ED-ALPHA</span>
        </Link>
        <nav className="hidden items-center gap-1 text-sm font-medium text-muted-foreground sm:flex">
          <Link href="/tutorial" className="rounded-md px-3 py-2 transition hover:bg-secondary hover:text-foreground">
            Tutorial
          </Link>
          <Link href="/walkthrough" className="rounded-md px-3 py-2 transition hover:bg-secondary hover:text-foreground">
            Walkthrough
          </Link>
          <a
            href={GITHUB_URL}
            target="_blank"
            rel="noreferrer"
            className="rounded-md px-3 py-2 transition hover:bg-secondary hover:text-foreground"
          >
            GitHub
          </a>
        </nav>
      </header>

      <section className="mx-auto grid w-full max-w-7xl gap-8 px-5 pb-12 pt-8 sm:px-8 sm:pb-16 sm:pt-10 lg:grid-cols-[0.92fr_1.08fr] lg:items-center lg:pb-20 lg:pt-18">
        <div className="mx-auto max-w-3xl text-center lg:mx-0 lg:text-left">
          <h1 className="text-5xl font-black leading-[1.02] text-slate-950 sm:text-6xl lg:text-7xl">
            ED-Alpha
          </h1>
          <p className="mx-auto mt-5 max-w-2xl text-xl font-medium leading-8 text-slate-700 lg:mx-0">
            Event-Driven ALPHA (ED-Alpha) is an open benchmark for predicting future, investment-relevant corporate events.
          </p>
        </div>

        <div className="relative -mx-5 sm:mx-0">
          <div className="overflow-hidden bg-slate-950 shadow-[0_24px_70px_rgba(15,23,42,0.18)] sm:rounded-2xl sm:border sm:border-slate-200 lg:shadow-[0_30px_90px_rgba(15,23,42,0.18)]">
            <div className="flex items-center justify-between border-b border-white/10 bg-slate-900 px-4 py-2.5 sm:py-3">
              <div className="flex items-center gap-2" aria-hidden="true">
                <span className="size-2.5 rounded-full bg-red-400 sm:size-3" />
                <span className="size-2.5 rounded-full bg-amber-300 sm:size-3" />
                <span className="size-2.5 rounded-full bg-emerald-400 sm:size-3" />
              </div>
              <span className="text-xs font-semibold text-white/60">Earlier walkthrough · Sum scoring</span>
            </div>
            <video
              className="block aspect-video w-full bg-slate-950"
              controls
              playsInline
              preload="metadata"
              aria-label="ED-Alpha demo walkthrough video"
            >
              <source src={VIDEO_SRC} type="video/mp4" />
              Your browser does not support the video tag.
            </video>
          </div>
          <p className="px-5 pt-3 text-sm text-muted-foreground sm:px-0">
            This recording uses earlier sum-based scores.{" "}
            <Link href="/tutorial" className="font-medium text-primary underline underline-offset-4">
              Explore the paper’s averaged results in the interactive tutorial.
            </Link>
          </p>
        </div>
      </section>

      <section className="mx-auto w-full max-w-7xl px-5 pb-12 sm:px-8 sm:pb-16 lg:pb-20">
        <div className="mb-5 flex flex-col gap-2 text-center sm:mb-7 sm:flex-row sm:items-end sm:justify-between sm:text-left">
          <div>
            <h2 className="text-2xl font-black leading-tight text-slate-950 sm:text-4xl">Explore ED-Alpha</h2>
          </div>
        </div>
        <div className="grid overflow-hidden rounded-2xl border border-border bg-white shadow-[0_24px_80px_rgba(15,23,42,0.08)] md:grid-cols-3">
          <Link
            href="/tutorial"
            className="group flex items-center justify-between gap-4 border-b border-border p-4 transition hover:bg-secondary/70 sm:p-5 md:min-h-48 md:flex-col md:items-stretch md:p-6 md:border-b-0 md:border-r"
          >
            <div className="flex min-w-0 items-center gap-4 md:block">
              <div className="flex size-10 shrink-0 items-center justify-center rounded-xl bg-primary text-primary-foreground md:mb-8 md:size-11">
                <MousePointerClick className="size-5" />
              </div>
              <div className="min-w-0">
                <div className="hidden text-sm font-semibold uppercase text-muted-foreground md:block">Interactive</div>
                <h3 className="text-lg font-black text-slate-950 md:mt-2 md:text-2xl">Tutorial</h3>
              </div>
            </div>
            <span className="inline-flex shrink-0 items-center gap-2 text-sm font-bold text-primary md:mt-8">
              <span className="hidden sm:inline">Open dashboard</span>
              <ArrowRight className="size-4 transition group-hover:translate-x-1" />
            </span>
          </Link>
          <Link
            href="/walkthrough"
            className="group flex items-center justify-between gap-4 border-b border-border p-4 transition hover:bg-secondary/70 sm:p-5 md:min-h-48 md:flex-col md:items-stretch md:p-6 md:border-b-0 md:border-r"
          >
            <div className="flex min-w-0 items-center gap-4 md:block">
              <div className="flex size-10 shrink-0 items-center justify-center rounded-xl bg-[oklch(0.55_0.15_85)] text-white md:mb-8 md:size-11">
                <PlayCircle className="size-5" />
              </div>
              <div className="min-w-0">
                <div className="hidden text-sm font-semibold uppercase text-muted-foreground md:block">Guided</div>
                <h3 className="text-lg font-black text-slate-950 md:mt-2 md:text-2xl">Walkthrough</h3>
              </div>
            </div>
            <span className="inline-flex shrink-0 items-center gap-2 text-sm font-bold text-primary md:mt-8">
              <span className="hidden sm:inline">Watch HTML demo</span>
              <ArrowRight className="size-4 transition group-hover:translate-x-1" />
            </span>
          </Link>
          <a
            href={GITHUB_URL}
            target="_blank"
            rel="noreferrer"
            className="group flex items-center justify-between gap-4 p-4 transition hover:bg-secondary/70 sm:p-5 md:min-h-48 md:flex-col md:items-stretch md:p-6"
          >
            <div className="flex min-w-0 items-center gap-4 md:block">
              <div className="flex size-10 shrink-0 items-center justify-center rounded-xl bg-slate-950 md:mb-8 md:size-11">
                <img src={GITHUB_ICON_SRC} alt="" className="size-5 invert" />
              </div>
              <div className="min-w-0">
                <div className="hidden text-sm font-semibold uppercase text-muted-foreground md:block">Source</div>
                <h3 className="text-lg font-black text-slate-950 md:mt-2 md:text-2xl">GitHub</h3>
              </div>
            </div>
            <span className="inline-flex shrink-0 items-center gap-2 text-sm font-bold text-primary md:mt-8">
              <span className="hidden sm:inline">View repository</span>
              <ExternalLink className="size-4 transition group-hover:translate-x-1" />
            </span>
          </a>
        </div>
      </section>
    </main>
  )
}
