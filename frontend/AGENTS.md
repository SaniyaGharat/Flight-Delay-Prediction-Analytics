<!-- LOVABLE:BEGIN -->
> [!IMPORTANT]
> This project is connected to [Lovable](https://lovable.dev). Avoid rewriting
> published git history — force pushing, or rebasing/amending/squashing commits
> that are already pushed — as it rewrites history on Lovable's side and the
> user will likely lose their project history.
>
> Commits you push to the connected branch sync back to Lovable and show up in
> the editor, so keep the branch in a working state.
<!-- LOVABLE:END -->

## Architecture rules
- No backend: data files are fetched from /public/data at runtime and aggregated in the browser (src/lib/data.ts loaders via react-query). Why: user requirement, static dataset.
- All rate math lives in src/lib/metrics.ts and is computed from summed counts. Why: averaging rates gives wrong answers.
- Global filters live in src/lib/filters.tsx context mounted in __root; pages filter rows with matchesDate/matchesAirline. Why: one shared filter state across pages.
- Shared panel/KPI/table/tooltip primitives live in src/components/dash/ui.tsx. Why: consistent editorial look.
