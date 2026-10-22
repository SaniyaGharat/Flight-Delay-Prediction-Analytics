import { useState, type ReactNode } from "react";
import { Link, useRouterState } from "@tanstack/react-router";
import { Menu } from "lucide-react";
import { Sheet, SheetContent, SheetTitle } from "@/components/ui/sheet";
import { FilterBar } from "./FilterBar";
import { cn } from "@/lib/utils";

const NAV = [
  { to: "/", label: "Overview", n: "01" },
  { to: "/airlines", label: "Airlines", n: "02" },
  { to: "/airports", label: "Airports and routes", n: "03" },
  { to: "/causes", label: "Causes and cancellations", n: "04" },
  { to: "/prediction", label: "Prediction", n: "05" },
  { to: "/pipeline", label: "Pipeline", n: "06" },
] as const;

const NO_FILTERS = ["/prediction", "/pipeline"];

function Nav({ onNavigate }: { onNavigate?: () => void }) {
  const path = useRouterState({ select: (s) => s.location.pathname });
  return (
    <nav className="flex flex-col gap-0.5">
      {NAV.map((item) => {
        const active = path === item.to;
        return (
          <Link
            key={item.to}
            to={item.to}
            onClick={onNavigate}
            className={cn(
              "flex items-baseline gap-3 rounded-lg px-3 py-2 text-sm transition-colors",
              active ? "bg-card text-primary shadow-card ring-1 ring-border" : "text-muted-foreground hover:text-foreground",
            )}
          >
            <span className="font-display text-xs opacity-70">{item.n}</span>
            {item.label}
          </Link>
        );
      })}
    </nav>
  );
}

function Brand() {
  return (
    <div className="px-3">
      <p className="font-display text-xl leading-tight text-foreground">Flight Delay Analytics</p>
      <p className="mt-1 text-xs text-muted-foreground">US domestic flights, 2015</p>
    </div>
  );
}

export function AppShell({ children }: { children: ReactNode }) {
  const [open, setOpen] = useState(false);
  const path = useRouterState({ select: (s) => s.location.pathname });
  const showFilters = !NO_FILTERS.includes(path);

  return (
    <div className="flex min-h-screen w-full">
      <aside className="sticky top-0 hidden h-screen w-64 shrink-0 flex-col gap-8 border-r border-border px-3 py-8 lg:flex">
        <Brand />
        <Nav />
        <p className="mt-auto px-3 text-xs leading-relaxed text-muted-foreground">5.8M flights, 14 carriers, 12 months.</p>
      </aside>

      <Sheet open={open} onOpenChange={setOpen}>
        <SheetContent side="left" className="w-72 bg-background p-6">
          <SheetTitle className="sr-only">Navigation</SheetTitle>
          <div className="flex flex-col gap-8">
            <Brand />
            <Nav onNavigate={() => setOpen(false)} />
          </div>
        </SheetContent>
      </Sheet>

      <div className="flex min-w-0 flex-1 flex-col">
        <div className="sticky top-0 z-20 border-b border-border bg-background/95 px-5 py-3 backdrop-blur-none md:px-10">
          <div className="flex items-center gap-3">
            <button
              className="rounded-md border border-border p-1.5 text-muted-foreground lg:hidden"
              onClick={() => setOpen(true)}
              aria-label="Open navigation"
            >
              <Menu className="h-4 w-4" strokeWidth={1.5} />
            </button>
            <span className="font-display text-base lg:hidden">Flight Delay Analytics</span>
          </div>
          {showFilters ? (
            <div className="mt-3 lg:mt-0">
              <FilterBar />
            </div>
          ) : (
            <p className="mt-3 text-xs text-muted-foreground lg:mt-0 lg:py-1.5">Filters do not apply to this page.</p>
          )}
        </div>

        <main className="flex-1 px-5 py-8 md:px-10 md:py-10">{children}</main>

        <footer className="border-t border-border px-5 py-6 text-xs text-muted-foreground md:px-10">
          Source: US DOT on-time performance 2015. Processed with PySpark on HDFS.
        </footer>
      </div>
    </div>
  );
}
