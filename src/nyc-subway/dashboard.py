import marimo

__generated_with = "0.12.8"
app = marimo.App(width="medium")


@app.cell
def _():
    # docker cp data/nyc-subway/gtfs_supplemented silly_maxwell:/app/data/gtfs_supplemented
    # docker exec -u root silly_maxwell chmod -R 755 /app
    return


@app.cell
def _():
    import pandas as pd
    import altair as alt
    import marimo as mo
    import os
    return alt, mo, os, pd


@app.cell
def _(alt, full_stop_times):
    def get_eda_summary(selected_routes):
        # Filter data based on selected subway routes
        filtered_st = full_stop_times[
            full_stop_times["route_id"].isin(selected_routes)
        ]

        # Summary Table (route-level)
        summary = (
            filtered_st
            .groupby("route_id")
            .agg(
                total_arrivals=("trip_id", "count"),
                unique_stops=("stop_id", "nunique"),
                directions=("direction_id", "nunique"),
            )
            .reset_index()
        )

        # Hourly Trend Chart
        hourly_trend = (
            filtered_st
            .groupby(["hour", "route_id"])
            .size()
            .reset_index(name="arrivals")
        )

        trend_chart = (
            alt.Chart(hourly_trend)
            .mark_line(interpolate="basis")
            .encode(
                x=alt.X("hour:Q", title="Hour of Day"),
                y=alt.Y("arrivals:Q", title="Scheduled Arrivals"),
                color=alt.Color("route_id:N", title="Subway Route"),
                tooltip=["hour", "route_id", "arrivals"],
            )
            .properties(
                height=200,
                title="System-Wide Hourly Subway Pulse",
            )
        )

        return summary, trend_chart

    return (get_eda_summary,)


@app.cell
def _(mo, os, pd):
    GTFS_PATH = os.path.join("data", "gtfs_supplemented")

    @mo.cache
    def load_and_preprocess():
        """Load MTA Subway GTFS snapshot for EDA and mapping"""

        # Stops
        stops = pd.read_csv(
            os.path.join(GTFS_PATH, "stops.txt"),
            usecols=["stop_id", "stop_name", "stop_lat", "stop_lon", "parent_station"],
        )

        # Stop times
        stop_times = pd.read_csv(
            os.path.join(GTFS_PATH, "stop_times.txt"),
            usecols=["trip_id", "stop_id", "arrival_time"],
        )

        # Trips
        trips = pd.read_csv(
            os.path.join(GTFS_PATH, "trips.txt"),
            usecols=["trip_id", "route_id", "direction_id", "shape_id"],
        )

        # Routes (THIS WAS MISSING)
        routes = pd.read_csv(
            os.path.join(GTFS_PATH, "routes.txt"),
            usecols=["route_id", "route_color", "route_short_name"],
        )

        # Normalize route_color (ensure leading #)
        routes["route_color"] = (
            "#" + routes["route_color"].str.strip()
        )

        # Merge stop_times ↔ trips
        stop_times = stop_times.merge(trips, on="trip_id", how="left")

        # Merge routes to inject route_color
        stop_times = stop_times.merge(
            routes[["route_id", "route_color"]],
            on="route_id",
            how="left",
        )

        # Extract hour (00–23)
        stop_times["hour"] = stop_times["arrival_time"].str.slice(0, 2).astype(int)

        return stops, stop_times, routes


    full_stops, full_stop_times, routes = load_and_preprocess()
    return GTFS_PATH, full_stop_times, full_stops, load_and_preprocess, routes


@app.cell
def _(full_stop_times, mo):
    # Hour selector
    hour_options = {
        f"{h:02d}:00 - {(h + 1):02d}:00": h
        for h in range(24)
    }

    time_dropdown = mo.ui.dropdown(
        options=hour_options,
        value="08:00 - 09:00",
        label="Select Hour"
    )

    # Route selector (derived from GTFS)
    route_ids = sorted(full_stop_times["route_id"].dropna().unique())

    route_selector = mo.ui.multiselect(
        options=route_ids,
        value=route_ids,
        label="Include Subway Routes"
    )

    return hour_options, route_ids, route_selector, time_dropdown


@app.cell
def _(alt, full_stop_times, full_stops, mo):
    def get_city_map(selected_hour, selected_routes):
        # 1. Guard clauses
        if not selected_routes:
            return mo.md("### Select at least one subway route to view the map.")

        h_data = full_stop_times[
            (full_stop_times["hour"] == selected_hour)
            & (full_stop_times["route_id"].isin(selected_routes))
        ]

        if h_data.empty:
            return mo.md(f"### No subway arrivals found for {selected_hour:02d}:00.")

        # 2. Aggregate stop-level activity
        counts = (
            h_data
            .groupby(["stop_id", "route_id", "route_color"])
            .size()
            .reset_index(name="stop_count")
        )

        map_data = counts.merge(
            full_stops[["stop_id", "stop_name", "stop_lat", "stop_lon"]],
            on="stop_id",
            how="left",
        )

        # 3. Build map chart using GTFS route_color
        chart = (
            alt.Chart(map_data.sample(min(len(map_data), 12000)))
            .mark_circle(opacity=0.35, stroke="black", strokeWidth=0.2)
            .encode(
                x=alt.X(
                    "stop_lon:Q",
                    scale=alt.Scale(domain=[-74.26, -73.70]),
                    axis=None,
                ),
                y=alt.Y(
                    "stop_lat:Q",
                    scale=alt.Scale(domain=[40.49, 40.92]),
                    axis=None,
                ),
                size=alt.Size(
                    "stop_count:Q",
                    scale=alt.Scale(type="pow", exponent=2.5, range=[10, 2000]),
                    legend=alt.Legend(title="Scheduled Trains"),
                ),
                color=alt.Color(
                    "route_color:N",
                    scale=None,  # 👈 critical: disables Vega palette
                    legend=alt.Legend(title="Subway Route"),
                ),
                tooltip=[
                    "stop_name:N",
                    "route_id:N",
                    "stop_count:Q",
                ],
            )
            .properties(width="container", height=800)
            .configure_view(stroke=None)
            .interactive()
        )

        return mo.ui.altair_chart(chart)
    return (get_city_map,)


@app.cell
def _(full_stop_times, full_stops):
    def get_top_stops(selected_hour, selected_routes):
        h_data = full_stop_times[
            (full_stop_times["hour"] == selected_hour)
            & (full_stop_times["route_id"].isin(selected_routes))
        ]

        top_stops = (
            h_data
            .groupby("stop_id")
            .size()
            .reset_index(name="arrivals")
            .merge(
                full_stops[["stop_id", "stop_name"]],
                on="stop_id",
                how="left",
            )
            .sort_values("arrivals", ascending=False)
            .head(10)
        )

        return top_stops

    return (get_top_stops,)


@app.cell
def _(get_eda_summary, mo, route_selector):
    summary_df, trend_viz = get_eda_summary(route_selector.value)

    selection_status = mo.stat(
        label="Active Routes",
        value=", ".join(route_selector.value) if route_selector.value else "None",
        caption=f"Analyzing {len(route_selector.value)} subway routes",
    )
    return selection_status, summary_df, trend_viz


@app.cell
def _(full_stop_times, route_selector, time_dropdown):
    # 1. Get data for the selected hour and the previous hour
    current_hour = time_dropdown.value
    prev_hour = (current_hour - 1) % 24

    # Filter data
    curr_filt = full_stop_times[
        (full_stop_times["hour"] == current_hour)
        & (full_stop_times["route_id"].isin(route_selector.value))
    ]

    prev_filt = full_stop_times[
        (full_stop_times["hour"] == prev_hour)
        & (full_stop_times["route_id"].isin(route_selector.value))
    ]
    return curr_filt, current_hour, prev_filt, prev_hour


@app.cell
def _(curr_filt, prev_filt):
    # 2. Calculation Helper
    def get_rate(current, previous):
        if previous == 0:
            return 0
        return (current - previous) / previous


    # Total Arrivals
    curr_arrivals = len(curr_filt)
    prev_arrivals = len(prev_filt)
    arrival_rate = get_rate(curr_arrivals, prev_arrivals)

    # Active Routes
    curr_routes = curr_filt["route_id"].nunique()
    prev_routes = prev_filt["route_id"].nunique()
    route_rate = get_rate(curr_routes, prev_routes)

    # Active Stops
    curr_stops = curr_filt["stop_id"].nunique()
    prev_stops = prev_filt["stop_id"].nunique()
    stop_rate = get_rate(curr_stops, prev_stops)
    return (
        arrival_rate,
        curr_arrivals,
        curr_routes,
        curr_stops,
        get_rate,
        prev_arrivals,
        prev_routes,
        prev_stops,
        route_rate,
        stop_rate,
    )


@app.cell
def _(
    arrival_rate,
    curr_arrivals,
    curr_routes,
    curr_stops,
    mo,
    route_rate,
    stop_rate,
):
    # 3. Create Stat Widgets
    total_arrivals_stat = mo.stat(
        label="Total Arrivals",
        bordered=True,
        caption=f"{arrival_rate:+.0%} vs last hour",
        direction="increase" if arrival_rate >= 0 else "decrease",
        value=f"{curr_arrivals:,}",
    )

    active_routes_stat = mo.stat(
        label="Active Routes",
        bordered=True,
        caption=f"{route_rate:+.0%}",
        direction="increase" if route_rate >= 0 else "decrease",
        value=str(curr_routes),
    )

    active_stops_stat = mo.stat(
        label="Active Stops",
        bordered=True,
        caption=f"{stop_rate:+.0%}",
        direction="increase" if stop_rate >= 0 else "decrease",
        value=f"{curr_stops:,}",
    )

    stats_row = mo.hstack(
        [total_arrivals_stat, active_routes_stat, active_stops_stat],
        widths="equal",
        gap=1,
    )
    return (
        active_routes_stat,
        active_stops_stat,
        stats_row,
        total_arrivals_stat,
    )


@app.cell
def _(
    get_city_map,
    mo,
    route_selector,
    stats_row,
    summary_df,
    time_dropdown,
    trend_viz,
):
    mo.vstack([
        mo.md("# NYC Subway Intelligence Dashboard"),

        # Controls
        mo.callout(
            mo.hstack(
                [time_dropdown, route_selector],
                justify="center",
                gap=6,
            ),
            kind="info",
        ),

        # Metrics
        mo.md("## Subway Metrics"),
        stats_row,

        # Tables and Charts
        mo.md("### Route Activity"),
        mo.ui.table(summary_df),

        mo.md("### Hourly Subway Pulse"),
        trend_viz,

        # Controls (repeat for UX symmetry)
        mo.callout(
            mo.hstack(
                [time_dropdown, route_selector],
                justify="center",
                gap=6,
            ),
            kind="info",
        ),

        # Map (still stop-based for now)
        mo.md("## Live Station Density Map"),
        get_city_map(time_dropdown.value, route_selector.value),
    ])
    return


if __name__ == "__main__":
    app.run()
