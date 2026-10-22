# Flight Delay Atlas

Build a multi-page analytics dashboard called "Flight Delay Analytics" for a big data project about US domestic flights in 2015 (5.8M flights). It must look like a calm, hand-designed editorial analytics product, not a generic AI template.

STACK
React + TypeScript + Tailwind + shadcn/ui + Recharts + react-router + PapaParse.
No backend. Load CSV/JSON files from /public/data at runtime and compute everything in the browser. Show skeleton loaders, and a clear error state if a file fails to load.

DESIGN (important)
- Soft pastel palette only. No neon, no saturated colours, no gradients on backgrounds, no glassmorphism, no glowing shadows, no emoji, no purple-to-blue hero gradients.
- Background warm off-white #FAF8F5, cards #FFFFFF with a 1px border #ECE7E0 and very subtle shadow. Text #3A3A3C, muted text #7A7770.
- Chart palette (muted pastels): sage #A8C3B0, dusty blue #A9C1D9, soft peach #F2C6A8, lavender grey #C9C1DB, butter #EBDDA6, blush #E8B9B9, stone #D6D0C4.
- One accent for emphasis: deep slate blue #5B7088, used sparingly for active states, selected nav items and key numbers.
- Typography: "DM Sans" for UI, "Fraunces" (or Newsreader) for page titles and big numbers. Generous whitespace, 12px radius, thin lines, no heavy bold. Left sidebar navigation, collapsible on mobile. Fully responsive.
- Charts: thin axes, light dotted gridlines, no 3D, rounded bars, tooltips with real numbers. Every chart has a one-sentence plain-English takeaway under its title, and axis labels with units.
- Write copy like an analyst, not marketing. No "revolutionize", "unlock", "seamless".

DATA FILES (in /public/data)
- dim_date.csv: DATE, YEAR, MONTH, MONTH_NAME, DAY, DAY_OF_WEEK, DAY_OF_WEEK_NAME, WEEK_OF_YEAR, IS_WEEKEND, IS_HOLIDAY_WINDOW
- dim_airline.csv: code, name
- fact_daily_airline.csv: FLIGHT_DATE, AIRLINE, total_flights, flown_flights, cancelled_flights, diverted_flights, delayed_arr_15, delayed_dep_15, sum_arr_delay_min, sum_dep_delay_min, sum_delay_minutes_pos, sum_cause_air_system, sum_cause_security, sum_cause_airline, sum_cause_late_aircraft, sum_cause_weather
- fact_airline_dow_hour.csv: AIRLINE, DAY_OF_WEEK (1=Mon), SCHED_DEP_HOUR, total_flights, flown_flights, cancelled_flights, diverted_flights, delayed_arr_15, delayed_dep_15, sum_arr_delay_min, sum_dep_delay_min, sum_delay_minutes_pos
- fact_route_month.csv: ROUTE, ORIGIN, DEST, MONTH, total_flights, flown_flights, cancelled_flights, diverted_flights, delayed_arr_15, delayed_dep_15, sum_arr_delay_min, sum_dep_delay_min, sum_delay_minutes_pos
- fact_cause_daily_airline.csv: FLIGHT_DATE, AIRLINE, CAUSE, MINUTES, FLIGHTS
- fact_cancel_daily_airline_reason.csv: FLIGHT_DATE, AIRLINE, CANCELLATION_REASON (A=Airline, B=Weather, C=National Air System, D=Security), CANCELLED_FLIGHTS
- airport_origin_totals.csv and airport_dest_totals.csv: code, total_flights, flown_flights, cancelled_flights, diverted_flights, delayed_arr_15, sum_arr_delay_min, name, city, state, lat, lon, HAS_COORDS
- ml_summary.json: split info and per-model auc_roc, auc_pr, tn, fp, fn, tp
Empty cells mean 0. Parse numbers and dates safely.

METRIC DEFINITIONS (use exactly these; always compute rates from summed counts, never by averaging rates)
- Cancel rate = cancelled / total
- Divert rate = diverted / total
- Delay rate (arrival >= 15 min) = delayed_arr_15 / flown
- On-time rate = 1 - delay rate (blank if flown = 0)
- Avg arrival delay = sum_arr_delay_min / flown
- Cause share = cause minutes / total cause minutes
Ignore the airport code "UNMAPPED" in maps and rankings, but mention it in a footnote.
Rankings of airports need flown >= 1000, routes flown >= 500.

GLOBAL FILTERS (top bar, shared across pages when relevant)
Month range, airline multi-select, weekday/weekend, holiday-window toggle. Filters must update every chart on the page.

PAGES
1. Overview: KPI cards (total flights, cancel rate, delay rate, on-time rate, avg arrival delay), monthly delay-rate line with total flights as soft bars behind, a calendar heatmap of daily delay rate for 2015, and a holiday-window vs normal days comparison.
2. Airlines: ranked horizontal bars of on-time rate with flown flights in the tooltip, a sortable table (flights, cancel rate, delay rate, avg delay), an airline x month heatmap, and an hour-of-day x weekday heatmap of delay rate from fact_airline_dow_hour.
3. Airports and routes: a US scatter/bubble map using lat/lon (use a simple SVG US projection or react-simple-maps, pastel fill) with bubble size = flights and colour = delay rate, an origin vs destination toggle, a top/bottom 10 airport table, and a route table with a search box.
4. Delay causes and cancellations: donut of cause share, stacked monthly cause minutes, cancellation reasons by month, and by airline. Add a note that cause minutes are only recorded for flights delayed 15 minutes or more.
5. Prediction: show the two models from ml_summary.json (logistic regression and gradient boosted trees): AUC-ROC, AUC-PR, derived precision, recall, F1, accuracy, and a confusion-matrix heatmap for each. Include a plain-language callout: "Schedule-only features give weak prediction (AUC about 0.61 to 0.62). Weather and the inbound aircraft's delay are not in the data." Show the chronological split (train Jan to Sep, validation Oct, test Nov to Dec). Do not claim the models are accurate.
6. Pipeline: a horizontal diagram of the architecture: Raw CSV -> HDFS -> PySpark cleaning -> enrichment -> analytics tables -> Parquet/CSV export -> this dashboard. Below it, a table of stages with row counts: raw 5,819,079 rows, flights_clean 5,819,079, flights_enriched 5,819,079 (70 columns), flown flights 5,714,008, cancelled 89,884, diverted 15,187. Add a "Data quality notes" card: October airport codes were inferred from other months (296 of 307 IDs mapped, 11 flagged as UNMAPPED); one year of data only.

SMALL DETAILS
- Number formatting with thousands separators and 1 decimal for percentages.
- A "Download this table as CSV" button on every table.
- Empty and loading states that match the palette.
- Footer: "Source: US DOT on-time performance 2015. Processed with PySpark on HDFS."

Start with the layout, theme tokens and Overview page, using real data from /public/data.
Then build the remaining pages one at a time.

This project was built with [Lovable](https://lovable.dev).

## Build with Lovable

Continue developing this project in the [Lovable editor](https://lovable.dev/projects/3761a44f-08d7-48a8-8ff0-cbddd2bcfe4b).

- **Ship faster**: describe what you want to build and Lovable handles the code.
- **Stay in sync**: every change made in Lovable is committed straight to this repository.
- **Full ownership**: this code is yours. Push to `main` on GitHub and your changes sync back into Lovable, ready for your next prompt.

## Development

Prefer working locally? You need Node.js and npm — [install with nvm](https://github.com/nvm-sh/nvm#installing-and-updating).

```sh
git clone <this-repository-url>
cd <repository-name>
npm i
npm run dev
```
