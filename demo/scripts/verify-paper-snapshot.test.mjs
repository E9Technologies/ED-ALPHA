import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import test from "node:test"

const data = JSON.parse(readFileSync(new URL("../data/demo-data.json", import.meta.url), "utf8"))
const paper = {
  50: { model: "anthropic/claude-sonnet-4.5", recall: [0.08, 0.36, 0.60], precision: [0.80, 0.72, 0.60], f1: [0.15, 0.48, 0.60] },
  52: { model: "openai/gpt-5", recall: [0.08, 0.38, 0.64], precision: [0.80, 0.76, 0.64], f1: [0.15, 0.51, 0.64] },
  54: { model: "google/gemini-2.5-pro", recall: [0.10, 0.40, 0.68], precision: [1.00, 0.80, 0.68], f1: [0.18, 0.53, 0.68] },
}

test("all 27 displayed results match the submitted paper", () => {
  assert.deepEqual(data.experiment.run_ids, [50, 52, 54])
  assert.deepEqual(data.runs.map((run) => run.id), data.experiment.run_ids)
  assert.equal(data.provenance.aggregation, "mean_article_score")
  for (const run of data.runs) {
    assert.equal(run.model_name, paper[run.id].model)
    assert.deepEqual(run.metrics.map((metric) => metric.k), [5, 25, 50])
    for (const [index, metric] of run.metrics.entries()) {
      assert.equal(metric.precision, paper[run.id].precision[index])
      assert.equal(metric.recall, paper[run.id].recall[index])
      const f1 = 2 * metric.precision * metric.recall / (metric.precision + metric.recall)
      assert.equal(Number(f1.toFixed(2)), paper[run.id].f1[index])
    }
  }
})

test("rankings use unrounded averages and all 100 labelled companies", () => {
  for (const run of data.runs) {
    assert.equal(run.topResults.length, 100)
    assert.equal(new Set(run.topResults.map((row) => row.cik)).size, 100)
    assert.equal(run.topResults.filter((row) => row.label === 1).length, 50)
    for (const [index, row] of run.topResults.entries()) {
      const expected = row.article_count ? row.score_sum / row.article_count : 0
      assert.equal(row.average_score, expected)
      assert.ok(!("total_score" in row), "old sum scores must not be exported as averages")
      if (index > 0) {
        const previous = run.topResults[index - 1]
        const difference = previous.score_sum * (row.article_count || 1) - row.score_sum * (previous.article_count || 1)
        assert.ok(difference > 0 || (difference === 0 && previous.cik < row.cik))
      }
    }
    for (const metric of run.metrics) {
      const top = run.topResults.slice(0, metric.k)
      const tp = top.filter((row) => row.label === 1).length
      assert.deepEqual(metric.top_ciks, top.map((row) => row.cik))
      assert.deepEqual(metric.top_scores, top.map((row) => row.average_score))
      assert.equal(metric.positives_in_top, tp)
      assert.equal(metric.total_positives, 50)
      assert.equal(metric.precision, tp / metric.k)
      assert.equal(metric.recall, tp / 50)
    }
  }
})

test("Hit/Miss and linked filings agree with the labels and event window", () => {
  const start = data.experiment.predict_date
  const end = data.provenance.event_window_end
  assert.equal(start, "2025-09-01")
  assert.equal(end, "2025-09-05")
  for (const run of data.runs) {
    for (const row of run.topResults) {
      assert.equal(Boolean(row.event), row.label === 1)
      if (row.event) {
        assert.ok(row.event.filing_date >= start && row.event.filing_date <= end)
        assert.ok(row.event.items.length > 0)
        assert.ok(row.event.items.every((item) => data.experiment.item_codes.includes(item)))
        assert.equal(new URL(row.event.url).hostname, "www.sec.gov")
      }
      assert.equal(row.evidence.length, Math.min(row.article_count, 3))
      for (const evidence of row.evidence) {
        assert.ok(evidence.llm_score >= 1 && evidence.llm_score <= 5)
        assert.ok(evidence.title && evidence.summary)
        assert.ok(["http:", "https:"].includes(new URL(evidence.url).protocol))
      }
    }
  }
})
