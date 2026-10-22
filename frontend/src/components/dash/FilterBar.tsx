import { ChevronDown } from "lucide-react";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Checkbox } from "@/components/ui/checkbox";
import { useAirlines } from "@/lib/data";
import { MONTHS, useFilters, type DayType, type HolidayMode } from "@/lib/filters";
import { cn } from "@/lib/utils";

const selectCls =
  "h-8 rounded-md border border-border bg-card px-2 text-sm text-foreground focus:outline-none focus:ring-1 focus:ring-ring";

function Segmented<T extends string>({ value, options, onChange }: { value: T; options: [T, string][]; onChange: (v: T) => void }) {
  return (
    <div className="inline-flex rounded-md border border-border bg-card p-0.5">
      {options.map(([v, l]) => (
        <button
          key={v}
          onClick={() => onChange(v)}
          className={cn(
            "rounded px-2.5 py-1 text-xs transition-colors",
            value === v ? "bg-primary text-primary-foreground" : "text-muted-foreground hover:text-foreground",
          )}
        >
          {l}
        </button>
      ))}
    </div>
  );
}

export function FilterBar() {
  const { filters: f, setFilters, reset } = useFilters();
  const airlines = useAirlines();
  const set = (p: Partial<typeof f>) => setFilters({ ...f, ...p });
  const toggleAirline = (code: string) =>
    set({ airlines: f.airlines.includes(code) ? f.airlines.filter((a) => a !== code) : [...f.airlines, code] });

  const airlineLabel =
    f.airlines.length === 0 ? "All airlines" : f.airlines.length === 1 ? f.airlines[0] : `${f.airlines.length} airlines`;

  return (
    <div className="flex flex-wrap items-center gap-x-5 gap-y-3 text-sm">
      <label className="flex items-center gap-2 text-muted-foreground">
        <span className="text-xs">Months</span>
        <select className={selectCls} value={f.monthFrom} onChange={(e) => { const v = +e.target.value; set({ monthFrom: v, monthTo: Math.max(v, f.monthTo) }); }}>
          {MONTHS.map((m, i) => <option key={m} value={i + 1}>{m}</option>)}
        </select>
        <span className="text-xs">to</span>
        <select className={selectCls} value={f.monthTo} onChange={(e) => { const v = +e.target.value; set({ monthTo: v, monthFrom: Math.min(v, f.monthFrom) }); }}>
          {MONTHS.map((m, i) => <option key={m} value={i + 1}>{m}</option>)}
        </select>
      </label>

      <Popover>
        <PopoverTrigger className={cn(selectCls, "inline-flex items-center gap-2")}>
          {airlineLabel}
          <ChevronDown className="h-3.5 w-3.5 text-muted-foreground" strokeWidth={1.5} />
        </PopoverTrigger>
        <PopoverContent align="start" className="w-72 p-2">
          <div className="mb-1 flex justify-between px-2 py-1 text-xs text-muted-foreground">
            <span>Airlines</span>
            <button className="hover:text-foreground" onClick={() => set({ airlines: [] })}>Clear</button>
          </div>
          <div className="max-h-72 overflow-auto">
            {airlines.isLoading && <p className="px-2 py-2 text-xs text-muted-foreground">Loading…</p>}
            {airlines.isError && <p className="px-2 py-2 text-xs text-muted-foreground">dim_airline.csv could not be loaded.</p>}
            {airlines.data?.map((a) => (
              <label key={a.code} className="flex cursor-pointer items-center gap-2 rounded px-2 py-1.5 text-sm hover:bg-muted">
                <Checkbox checked={f.airlines.includes(a.code)} onCheckedChange={() => toggleAirline(a.code)} />
                <span className="w-7 text-xs text-muted-foreground">{a.code}</span>
                <span className="truncate">{a.name}</span>
              </label>
            ))}
          </div>
        </PopoverContent>
      </Popover>

      <Segmented<DayType> value={f.dayType} onChange={(v) => set({ dayType: v })} options={[["all", "All days"], ["weekday", "Weekdays"], ["weekend", "Weekends"]]} />
      <Segmented<HolidayMode> value={f.holiday} onChange={(v) => set({ holiday: v })} options={[["all", "Any period"], ["holiday", "Holiday windows"], ["normal", "Normal days"]]} />

      <button onClick={reset} className="text-xs text-muted-foreground underline-offset-4 hover:text-foreground hover:underline">
        Reset
      </button>
    </div>
  );
}
