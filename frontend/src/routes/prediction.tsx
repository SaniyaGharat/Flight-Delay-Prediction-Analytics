import { createFileRoute } from "@tanstack/react-router";
import { Fragment, useState } from "react";
import { useAirlines, useMlSummary, type MlModel } from "@/lib/data";
import { accuracy, f1Score, fmtInt, fmtPct, precision, recall } from "@/lib/metrics";
import {
  ChartSkeleton, DownloadButton, EmptyState, ErrorState, Kpi, KpiSkeleton, PageHeader, Panel, downloadCsv,
} from "@/components/dash/ui";

export const Route = createFileRoute("/prediction")({
  head: () => ({
    meta: [
      { title: "Prediction · Flight Delay Analytics" },
      { name: "description", content: "Schedule-only classifiers: AUC comparison and confusion matrices for logistic regression and gradient boosted trees." },
      { property: "og:title", content: "Prediction · Flight Delay Analytics" },
      { property: "og:description", content: "Schedule-only classifiers: AUC comparison and confusion matrices for logistic regression and gradient boosted trees." },
      { property: "og:type", content: "website" },
      { name: "twitter:card", content: "summary_large_image" },
    ],
  }),
  component: PredictionPage,
});

const DEFAULT_SPLIT: Record<string, string> = {
  train: "January-September 2015",
  validation: "October 2015",
  test: "November-December 2015",
};

const fmt2 = (n: number | null | undefined) => (n == null || !isFinite(n) ? "-" : n.toFixed(2));

function ConfusionMatrix({ tn, fp, fn, tp }: { tn: number; fp: number; fn: number; tp: number }) {
  const total = tn + fp + fn + tp;
  const cells = [
    { v: tn, correct: true, predicted: "on time", actual: 0 },
    { v: fp, correct: false, predicted: "delayed", actual: 0 },
    { v: fn, correct: false, predicted: "on time", actual: 1 },
    { v: tp, correct: true, predicted: "delayed", actual: 1 },
  ];
  const rows = ["Actually on time", "Actually delayed"];
  return (
    <div className="grid grid-cols-[minmax(96px,max-content)_1fr_1fr] gap-1.5 text-xs">
      <div />
      <div className="text-center text-[10px] text-muted-foreground">Predicted on time</div>
      <div className="text-center text-[10px] text-muted-foreground">Predicted delayed</div>
      {rows.map((rowLabel, ri) => (
        <Fragment key={rowLabel}>
          <div className="self-center pr-2 text-right text-[10px] text-muted-foreground">{rowLabel}</div>
          {cells
            .filter((c) => c.actual === ri)
            .map((c) => (
              <div
                key={`${ri}-${c.predicted}`}
                className="rounded-lg p-3 text-center"
                style={{ background: c.correct ? "var(--sage)" : "var(--blush)", opacity: 0.85 }}
              >
                <p className="font-display text-xl text-foreground">{fmtInt(c.v)}</p>
                <p className="text-[10px] text-muted-foreground">{fmtPct(total > 0 ? c.v / total : null)} of evaluated</p>
              </div>
            ))}
        </Fragment>
      ))}
    </div>
  );
}

function HistoricalDelayRateLookup() {
  const airlines = useAirlines();
  const [carrier, setCarrier] = useState("AA");
  const [origin, setOrigin] = useState("DFW");
  const [depHour, setDepHour] = useState(17);
  const [loading, setLoading] = useState(false);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);
  const [result, setResult] = useState<{
    label: string;
    flown_flights: number;
    delayed_arr_15: number;
    total_flights: number;
    delay_rate: number | null;
    has_sufficient_data: boolean;
    status: string;
    description: string;
  } | null>(null);

  const handleLookup = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setErrorMsg(null);
    try {
      const res = await fetch("/api/predict", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          airline: carrier.trim().toUpperCase(),
          origin: origin.trim().toUpperCase(),
          sched_dep_hour: Number(depHour),
        }),
      });
      if (res.ok) {
        const data = await res.json();
        setResult(data);
      } else {
        setErrorMsg(`API error: HTTP ${res.status}`);
        setResult(null);
      }
    } catch {
      setErrorMsg("Could not connect to backend API server. Please ensure python src/api/server.py is running.");
      setResult(null);
    } finally {
      setLoading(false);
    }
  };

  return (
    <Panel
      title="Historical delay rate (not a model forecast)"
      takeaway="Historical arrival delay rate (delay >= 15 min / flown flights) computed directly from 2015 BTS flight records."
      footnote="Shows 'no data' when fewer than 100 flown flights exist for the selected airline, origin airport, and departure hour."
    >
      <form onSubmit={handleLookup} className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <div>
          <label className="text-xs font-medium text-muted-foreground">Airline</label>
          <select
            value={carrier}
            onChange={(e) => setCarrier(e.target.value)}
            className="mt-1 w-full rounded-md border border-border bg-background px-3 py-2 text-sm text-foreground shadow-sm focus:outline-none focus:ring-1 focus:ring-primary"
          >
            {airlines.data && airlines.data.length > 0 ? (
              airlines.data.map((a) => (
                <option key={a.code} value={a.code}>
                  {a.code} - {a.name}
                </option>
              ))
            ) : (
              <>
                <option value="AA">AA - American Airlines Inc.</option>
                <option value="DL">DL - Delta Air Lines Inc.</option>
                <option value="UA">UA - United Air Lines Inc.</option>
                <option value="WN">WN - Southwest Airlines Co.</option>
                <option value="NK">NK - Spirit Air Lines</option>
              </>
            )}
          </select>
        </div>

        <div>
          <label className="text-xs font-medium text-muted-foreground">Origin Airport (IATA)</label>
          <input
            type="text"
            value={origin}
            maxLength={3}
            onChange={(e) => setOrigin(e.target.value.toUpperCase())}
            placeholder="DFW"
            className="mt-1 w-full rounded-md border border-border bg-background px-3 py-2 text-sm uppercase text-foreground shadow-sm focus:outline-none focus:ring-1 focus:ring-primary"
          />
        </div>

        <div>
          <label className="text-xs font-medium text-muted-foreground">Scheduled Departure Hour: {depHour}:00</label>
          <input
            type="range"
            min={0}
            max={23}
            value={depHour}
            onChange={(e) => setDepHour(Number(e.target.value))}
            className="mt-3 w-full accent-primary"
          />
        </div>

        <div className="flex flex-col justify-end">
          <button
            type="submit"
            disabled={loading}
            className="w-full rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground shadow transition hover:opacity-90 disabled:opacity-50"
          >
            {loading ? "Looking up..." : "Lookup Historical Rate"}
          </button>
        </div>
      </form>

      {errorMsg && (
        <div className="mt-4 rounded-lg border border-border bg-muted/40 p-3 text-xs text-muted-foreground">
          {errorMsg}
        </div>
      )}

      {result && (
        <div className="mt-5 rounded-lg border border-border bg-muted/30 p-4">
          <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
            {result.label}
          </p>

          {!result.has_sufficient_data || result.delay_rate == null ? (
            <div className="mt-3 flex items-center justify-between">
              <div>
                <span className="inline-block rounded-md bg-stone/50 px-2.5 py-1 font-mono text-xs font-semibold text-foreground">
                  no data
                </span>
                <p className="mt-1.5 text-xs text-muted-foreground">
                  Fewer than 100 flown flights recorded for {result.airline} at {result.origin} ({depHour}:00).
                </p>
              </div>
              <div className="text-right text-xs text-muted-foreground">
                <p>Flown flights: {fmtInt(result.flown_flights)}</p>
                <p>Delayed flights: {fmtInt(result.delayed_arr_15)}</p>
              </div>
            </div>
          ) : (
            <div className="mt-3">
              <div className="flex flex-wrap items-baseline justify-between gap-2">
                <div className="flex items-baseline gap-3">
                  <span className="font-display text-3xl text-foreground">
                    {fmtPct(result.delay_rate)}
                  </span>
                  <span className="text-xs text-muted-foreground">
                    historical arrival delay rate
                  </span>
                </div>
                <div className="text-right text-xs text-muted-foreground">
                  <p>
                    <span className="font-medium text-foreground">{fmtInt(result.delayed_arr_15)}</span> delayed /{" "}
                    <span className="font-medium text-foreground">{fmtInt(result.flown_flights)}</span> flown
                    {result.total_flights > result.flown_flights && ` (${fmtInt(result.total_flights)} scheduled)`}
                  </p>
                </div>
              </div>

              <div className="mt-3 h-2 w-full overflow-hidden rounded-full bg-border">
                <div
                  className="h-full rounded-full transition-all"
                  style={{
                    width: `${Math.min(100, Math.max(2, result.delay_rate * 100))}%`,
                    background: result.delay_rate > 0.25 ? "var(--blush)" : "var(--sage)",
                  }}
                />
              </div>
            </div>
          )}
        </div>
      )}
    </Panel>
  );
}

function PredictionPage() {
  const summary = useMlSummary();
  const split = summary.data ? { ...DEFAULT_SPLIT, ...summary.data.split } : DEFAULT_SPLIT;

  return (
    <div className="mx-auto max-w-6xl">
      <PageHeader
        eyebrow="05 · Prediction"
        title="Can schedule data predict a delay?"
        intro="Two classifiers — logistic regression and gradient boosted trees — were trained on schedule features only and evaluated on a chronological split, so the models always predict days they have never seen."
      />

      {summary.error ? (
        <ErrorState error={summary.error} />
      ) : (
        <div className="flex flex-col gap-6">
          <Panel title="Read this first" takeaway="What the models can and cannot see.">
            <div className="rounded-lg bg-muted/60 p-4">
              <p className="font-display text-lg leading-snug text-foreground">
                Schedule-only features give weak prediction (AUC about 0.61 to 0.62). Weather and the inbound aircraft's delay are not in the data.
              </p>
              <p className="mt-2 text-sm leading-relaxed text-muted-foreground">
                The two strongest signals of lateness are therefore invisible to these models. Read the figures below as a benchmark of what
                schedule data alone explains — not as an accurate delay predictor.
              </p>
            </div>
            <p className="mt-3 text-xs text-muted-foreground">
              Chronological split — train: {split["train"]} · validation: {split["validation"]} · test: {split["test"]}
            </p>
          </Panel>

          {summary.isLoading ? (
            <>
              <div className="grid gap-6 md:grid-cols-2">
                {Array.from({ length: 2 }).map((_, i) => (
                  <div key={i} className="rounded-xl border border-border bg-card p-5 shadow-card">
                    <ChartSkeleton height={200} />
                  </div>
                ))}
              </div>
              <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
                {Array.from({ length: 4 }).map((_, i) => <KpiSkeleton key={i} />)}
              </div>
            </>
          ) : (
            summary.data && (
              <>
                <div className="grid gap-6 md:grid-cols-2">
                  {summary.data.models.map((m) => (
                    <ModelCard key={m.name} model={m} />
                  ))}
                </div>

                <Panel
                  title="Model comparison"
                  takeaway="Both models land close together; the boosted trees buy a little ranking quality, not a different story."
                  actions={
                    <DownloadButton
                      onClick={() =>
                        downloadCsv(
                          "model_comparison.csv",
                          summary.data!.models.map((m) => ({
                            model: m.name,
                            auc_roc: m.aucRoc,
                            auc_pr: m.aucPr,
                            precision: precision(m.tp, m.fp),
                            recall: recall(m.tp, m.fn),
                            f1: f1Score(precision(m.tp, m.fp), recall(m.tp, m.fn)),
                            accuracy: accuracy(m.tp, m.fp, m.fn, m.tn),
                            tn: m.tn,
                            fp: m.fp,
                            fn: m.fn,
                            tp: m.tp,
                          })),
                        )
                      }
                    />
                  }
                  footnote="Precision, recall, F1 and accuracy are derived from the confusion counts in ml_summary.json."
                >
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="text-left text-xs text-muted-foreground">
                          <th className="pb-2 font-normal">Model</th>
                          <th className="pb-2 text-right font-normal">AUC-ROC</th>
                          <th className="pb-2 text-right font-normal">AUC-PR</th>
                          <th className="pb-2 text-right font-normal">Precision</th>
                          <th className="pb-2 text-right font-normal">Recall</th>
                          <th className="pb-2 text-right font-normal">F1</th>
                          <th className="pb-2 text-right font-normal">Accuracy</th>
                        </tr>
                      </thead>
                      <tbody>
                        {summary.data.models.map((m) => {
                          const p = precision(m.tp, m.fp);
                          const r = recall(m.tp, m.fn);
                          return (
                            <tr key={m.name} className="border-t border-border">
                              <td className="py-2">{m.name}</td>
                              <td className="py-2 text-right">{fmt2(m.aucRoc)}</td>
                              <td className="py-2 text-right">{fmt2(m.aucPr)}</td>
                              <td className="py-2 text-right">{fmtPct(p)}</td>
                              <td className="py-2 text-right">{fmtPct(r)}</td>
                              <td className="py-2 text-right">{fmtPct(f1Score(p, r))}</td>
                              <td className="py-2 text-right">{fmtPct(accuracy(m.tp, m.fp, m.fn, m.tn))}</td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                </Panel>

                <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
                  {summary.data.models.map((m) => (
                    <Kpi key={m.name} label={`${m.name} · AUC-ROC`} value={fmt2(m.aucRoc)} hint={`AUC-PR ${fmt2(m.aucPr)}`} emphasis={m === summary.data!.models[0]} />
                  ))}
                </div>

                <HistoricalDelayRateLookup />
              </>
            )
          )}
        </div>
      )}
    </div>
  );
}

function ModelCard({ model: m }: { model: MlModel }) {
  const total = m.tn + m.fp + m.fn + m.tp;
  return (
    <Panel
      title={m.name}
      takeaway={`AUC-ROC ${fmt2(m.aucRoc)} · AUC-PR ${fmt2(m.aucPr)} · ${fmtInt(total)} flights evaluated.`}
      footnote="Counts come straight from ml_summary.json; percentages are shares of the evaluated flights."
    >
      {total === 0 ? (
        <EmptyState>No confusion counts available for this model.</EmptyState>
      ) : (
        <ConfusionMatrix tn={m.tn} fp={m.fp} fn={m.fn} tp={m.tp} />
      )}
    </Panel>
  );
}

