import { createFileRoute } from "@tanstack/react-router";
import { DownloadButton, PageHeader, Panel, downloadCsv } from "@/components/dash/ui";
import { fmtInt } from "@/lib/metrics";

export const Route = createFileRoute("/pipeline")({
  head: () => ({
    meta: [
      { title: "Pipeline · Flight Delay Analytics" },
      { name: "description", content: "How 5.8M flight records moved from raw CSV through HDFS and PySpark into the tables behind this dashboard." },
      { property: "og:title", content: "Pipeline · Flight Delay Analytics" },
      { property: "og:description", content: "Architecture and row counts of the PySpark-on-HDFS pipeline behind the dashboard." },
    ],
  }),
  component: Pipeline,
});

const STEPS = [
  { name: "Raw CSV", note: "DOT on-time files", color: "var(--stone)" },
  { name: "HDFS", note: "Distributed storage", color: "var(--dusty)" },
  { name: "PySpark cleaning", note: "Types, nulls, codes", color: "var(--sage)" },
  { name: "Enrichment", note: "Dates, airports, flags", color: "var(--butter)" },
  { name: "Analytics tables", note: "Aggregated facts", color: "var(--peach)" },
  { name: "Parquet / CSV export", note: "Compact outputs", color: "var(--lavender)" },
  { name: "This dashboard", note: "Computed in browser", color: "var(--blush)" },
];

const STAGES = [
  { stage: "raw", description: "Rows read from source CSV", rows: 5819079, columns: null as number | null },
  { stage: "flights_clean", description: "After type casting and null handling", rows: 5819079, columns: null },
  { stage: "flights_enriched", description: "With date, airport and delay-cause features", rows: 5819079, columns: 70 },
  { stage: "flown flights", description: "Neither cancelled nor diverted", rows: 5714008, columns: null },
  { stage: "cancelled", description: "Cancelled flights", rows: 89884, columns: null },
  { stage: "diverted", description: "Diverted flights", rows: 15187, columns: null },
];

function Pipeline() {
  return (
    <div className="mx-auto max-w-6xl">
      <PageHeader
        eyebrow="06 · Pipeline"
        title="From raw files to this page"
        intro="The source data was loaded into HDFS, cleaned and enriched with PySpark, then aggregated into small analytics tables. The dashboard reads those tables and computes every rate in the browser."
      />
      <div className="flex flex-col gap-6">
        <Panel title="Architecture" takeaway="Each stage reads the output of the one before it; no row is dropped before aggregation.">
          <ol className="flex flex-col gap-3 md:flex-row md:items-stretch md:gap-0">
            {STEPS.map((s, i) => (
              <li key={s.name} className="flex items-center md:flex-1">
                <div className="flex-1 rounded-lg border border-border bg-card p-3">
                  <span className="mb-2 block h-1 w-8 rounded-full" style={{ background: s.color }} />
                  <p className={i === STEPS.length - 1 ? "text-sm text-primary" : "text-sm text-foreground"}>{s.name}</p>
                  <p className="mt-0.5 text-xs text-muted-foreground">{s.note}</p>
                </div>
                {i < STEPS.length - 1 && (
                  <span className="hidden px-1.5 text-muted-foreground md:block" aria-hidden>→</span>
                )}
              </li>
            ))}
          </ol>
        </Panel>

        <Panel
          title="Stages and row counts"
          takeaway="Cleaning and enrichment keep all 5,819,079 rows; 98.2% of scheduled flights were actually flown."
          actions={<DownloadButton onClick={() => downloadCsv("pipeline_stages.csv", STAGES)} />}
        >
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-xs text-muted-foreground">
                  <th className="pb-2 font-normal">Stage</th>
                  <th className="pb-2 font-normal">Description</th>
                  <th className="pb-2 text-right font-normal">Rows</th>
                  <th className="pb-2 text-right font-normal">Columns</th>
                </tr>
              </thead>
              <tbody>
                {STAGES.map((s) => (
                  <tr key={s.stage} className="border-t border-border">
                    <td className="py-2.5 font-mono text-xs">{s.stage}</td>
                    <td className="py-2.5 text-muted-foreground">{s.description}</td>
                    <td className="py-2.5 text-right">{fmtInt(s.rows)}</td>
                    <td className="py-2.5 text-right">{s.columns ?? "–"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Panel>

        <Panel title="Data quality notes">
          <ul className="flex flex-col gap-3 text-sm leading-relaxed">
            <li className="flex gap-3"><span className="mt-2 h-1.5 w-1.5 shrink-0 rounded-full bg-peach" />October 2015 airport codes arrived as numeric IDs. They were mapped back using the other months: 296 of 307 IDs matched, and 11 are flagged as UNMAPPED and left out of airport maps and rankings.</li>
            <li className="flex gap-3"><span className="mt-2 h-1.5 w-1.5 shrink-0 rounded-full bg-dusty" />The dataset covers one year only, so seasonal patterns cannot be checked against other years.</li>
            <li className="flex gap-3"><span className="mt-2 h-1.5 w-1.5 shrink-0 rounded-full bg-sage" />Delay-cause minutes are recorded only for flights that arrived 15 minutes or more late.</li>
          </ul>
        </Panel>
      </div>
    </div>
  );
}
