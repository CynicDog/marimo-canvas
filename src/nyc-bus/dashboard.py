import marimo

__generated_with = "0.12.8"
app = marimo.App(width="medium")


@app.cell
def _():
    ## Load data 
    # docker cp ./gtfs_b gifted_ride:/app/gtfs_b
    # docker cp ./gtfs_bx gifted_ride:/app/gtfs_bx
    # docker cp ./gtfs_m gifted_ride:/app/gtfs_m
    # docker cp ./gtfs_q gifted_ride:/app/gtfs_q
    # docker cp ./gtfs_si gifted_ride:/app/gtfs_si

    # docker exec -u root gifted_ride chmod -R 755 /app 
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
    def get_eda_summary(selected_boroughs):
        # Filter data based on UI selection
        filtered_st = full_stop_times[full_stop_times['borough'].isin(selected_boroughs)]
    
        # Summary Table
        summary = filtered_st.groupby('borough').agg(
            total_arrivals=('trip_id', 'count'),
            unique_routes=('route_id', 'nunique'),
            unique_stops=('stop_id', 'nunique')
        ).reset_index()

        # Hourly Trend Chart
        hourly_trend = filtered_st.groupby(['hour', 'borough']).size().reset_index(name='arrivals')
        trend_chart = alt.Chart(hourly_trend).mark_line(interpolate='basis').encode(
            x=alt.X('hour:Q', title="Hour of Day"),
            y=alt.Y('arrivals:Q', title="Scheduled Arrivals"),
            color='borough:N',
            tooltip=['hour', 'borough', 'arrivals']
        ).properties(height=200, title="City-Wide Hourly Pulse")

        return summary, trend_chart
    return (get_eda_summary,)


@app.cell
def _(mo):
    BOROUGHS = {
        'Brooklyn': 'gtfs_b', 'Bronx': 'gtfs_bx', 'Manhattan': 'gtfs_m',
        'Queens': 'gtfs_q', 'Staten Island': 'gtfs_si'
    }

    hour_options = {f"{h:02d}:00 - {(h+1):02d}:00": h for h in range(24)}
    time_dropdown = mo.ui.dropdown(options=hour_options, value='08:00 - 09:00', label="Select Hour")

    borough_selector = mo.ui.multiselect(
        options=list(BOROUGHS.keys()), 
        value=list(BOROUGHS.keys()), 
        label="Include Boroughs"
    )
    return BOROUGHS, borough_selector, hour_options, time_dropdown


@app.cell
def _(borough_selector, get_eda_summary, mo):
    summary_df, trend_viz = get_eda_summary(borough_selector.value)

    selection_status = mo.stat(
        label="Active Boroughs",
        value=", ".join(borough_selector.value) if borough_selector.value else "None",
        caption=f"Analyzing {len(borough_selector.value)} regions"
    )
    return selection_status, summary_df, trend_viz


@app.cell
def _(borough_selector, full_stop_times, time_dropdown):
    # 1. Get data for the selected hour and the previous hour
    current_hour = time_dropdown.value
    prev_hour = (current_hour - 1) % 24

    # Filter data
    curr_filt = full_stop_times[
        (full_stop_times['hour'] == current_hour) & 
        (full_stop_times['borough'].isin(borough_selector.value))
    ]
    prev_filt = full_stop_times[
        (full_stop_times['hour'] == prev_hour) & 
        (full_stop_times['borough'].isin(borough_selector.value))
    ]

    # 2. Calculation Helper
    def get_rate(current, previous):
        if previous == 0:
            return 0
        return (current - previous) / previous

    # Total Arrivals Stats
    curr_arrivals = len(curr_filt)
    prev_arrivals = len(prev_filt)
    arrival_rate = get_rate(curr_arrivals, prev_arrivals)

    # Unique Routes Stats
    curr_routes = curr_filt['route_id'].nunique()
    prev_routes = prev_filt['route_id'].nunique()
    route_rate = get_rate(curr_routes, prev_routes)

    # Active Stops Stats
    curr_stops = curr_filt['stop_id'].nunique()
    prev_stops = prev_filt['stop_id'].nunique()
    stop_rate = get_rate(curr_stops, prev_stops)
    return (
        arrival_rate,
        curr_arrivals,
        curr_filt,
        curr_routes,
        curr_stops,
        current_hour,
        get_rate,
        prev_arrivals,
        prev_filt,
        prev_hour,
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

    # Render the row
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
    borough_selector,
    get_city_map,
    mo,
    stats_row,
    summary_df,
    time_dropdown,
    trend_viz,
):
    mo.vstack([
        mo.md("# NYC Transit Intelligence Dashboard"),

        # Controls
        mo.callout(
            mo.hstack(
                [time_dropdown, borough_selector], 
                justify="center", 
                gap=6
            ),
            kind="info"
        ),
    
        # Movie-style Stats
        mo.md("## Transit Metrics"),
        stats_row,
    
        # Tables and Charts (Now stacked vertically)
        mo.md("### Borough Activity"),
        mo.ui.table(summary_df),
    
        mo.md("### Hourly Pulse"),
        trend_viz,
    
        # Controls
        mo.callout(
            mo.hstack(
                [time_dropdown, borough_selector], 
                justify="center", 
                gap=6
            ),
            kind="info"
        ),


        # Map
        mo.md("## Live Density Map"),
        get_city_map(time_dropdown.value, borough_selector.value)
    ])
    return


@app.cell
def _(BOROUGHS, alt, mo, os, pd):
    @mo.cache
    def load_and_preprocess():
        """Loads a snapshot of all boroughs for EDA and Mapping"""
        all_stops = []
        all_stop_times = []
    
        for b_name, path in BOROUGHS.items():
            if not os.path.exists(path): continue
        
            # Load essential columns
            s = pd.read_csv(os.path.join(path, "stops.txt"), usecols=['stop_id', 'stop_name', 'stop_lat', 'stop_lon'])
            st = pd.read_csv(os.path.join(path, "stop_times.txt"), usecols=['trip_id', 'stop_id', 'arrival_time'])
            tr = pd.read_csv(os.path.join(path, "trips.txt"), usecols=['trip_id', 'route_id'])
        
            # Tag and combine
            s['borough'] = b_name
            st = st.merge(tr, on='trip_id')
            st['borough'] = b_name
            st['hour'] = st['arrival_time'].str[:2].astype(int)
        
            all_stops.append(s)
            all_stop_times.append(st)
        
        return pd.concat(all_stops), pd.concat(all_stop_times)

    full_stops, full_stop_times = load_and_preprocess()

    def get_city_map(selected_hour, selected_boroughs):
        # 1. Guard Clauses (Return Markdown directly)
        if not selected_boroughs:
            return mo.md("### Select at least one borough to view the map.")

        h_data = full_stop_times[
            (full_stop_times['hour'] == selected_hour) & 
            (full_stop_times['borough'].isin(selected_boroughs))
        ]
    
        if h_data.empty:
            return mo.md(f"### No bus arrivals found for {selected_hour:02d}:00.")

        # 2. Data Processing
        counts = h_data.groupby(['stop_id', 'borough', 'route_id']).size().reset_index(name='stop_count')
        map_data = counts.merge(full_stops[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']], on='stop_id')

        # 3. Build Chart
        chart = alt.Chart(map_data.sample(min(len(map_data), 12000))).mark_circle(
            opacity=0.35, stroke='black', strokeWidth=0.2
        ).encode(
            x=alt.X('stop_lon:Q', scale=alt.Scale(domain=[-74.26, -73.70]), axis=None),
            y=alt.Y('stop_lat:Q', scale=alt.Scale(domain=[40.49, 40.92]), axis=None),
            size=alt.Size('stop_count:Q', 
                         scale=alt.Scale(type='pow', exponent=2.5, range=[10, 2000]), 
                         legend=alt.Legend(title="Scheduled Buses")),
            color=alt.Color('route_id:N', scale=alt.Scale(scheme='tableau20'), title="Route"),
            tooltip=['stop_name:N', 'route_id:N', 'stop_count:Q']
        ).properties(width="container", height=800).configure_view(stroke=None).interactive()

        # 4. WRAP IT HERE instead of in the layout
        return mo.ui.altair_chart(chart)
    
    def get_top_stops(selected_hour, selected_boroughs):
        h_data = full_stop_times[
            (full_stop_times['hour'] == selected_hour) & 
            (full_stop_times['borough'].isin(selected_boroughs))
        ]
        top_stops = h_data.groupby(['stop_id', 'borough']).size().reset_index(name='arrivals')
        top_stops = top_stops.merge(full_stops[['stop_id', 'stop_name']], on='stop_id')
        return top_stops.sort_values('arrivals', ascending=False).head(10)
    return (
        full_stop_times,
        full_stops,
        get_city_map,
        get_top_stops,
        load_and_preprocess,
    )


if __name__ == "__main__":
    app.run()
