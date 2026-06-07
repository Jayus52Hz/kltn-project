import json
from datetime import datetime

from superset.app import create_app
from superset.extensions import db


PROJECT_ID = "project-ef0c6db5-0765-4391-845"
SCHEMA = "kltn0710"
TABLE = "vw_telesales_performance"
DB_NAME = "BigQuery - KLTN Telesales"
DASHBOARD_TITLE = "Telesales BigQuery Dashboard"


def metric_obj(name, expression, label):
    return {
        "expressionType": "SQL",
        "sqlExpression": expression,
        "label": label,
        "optionName": f"metric_{name}",
    }


def upsert_metric(dataset, admin, name, expression, label, d3format):
    from superset.connectors.sqla.models import SqlMetric

    metric = next((item for item in dataset.metrics if item.metric_name == name), None)
    if metric is None:
        metric = SqlMetric(metric_name=name, table=dataset)
        db.session.add(metric)
    metric.expression = expression
    metric.verbose_name = label
    metric.d3format = d3format
    metric.created_by = metric.changed_by = admin
    return metric


def chart_node(chart, width, height, parents):
    node_id = f"CHART-{chart.id}"
    return node_id, {
        "children": [],
        "id": node_id,
        "meta": {"chartId": chart.id, "height": height, "width": width},
        "parents": parents,
        "type": "CHART",
    }


def main():
    app = create_app()
    with app.app_context():
        from superset.models.core import Database
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.slice import Slice
        from superset.models.dashboard import Dashboard

        admin = app.appbuilder.sm.find_user(username="admin")

        database = db.session.query(Database).filter_by(database_name=DB_NAME).one_or_none()
        if database is None:
            database = Database(database_name=DB_NAME)
            db.session.add(database)
        database.set_sqlalchemy_uri(f"bigquery://{PROJECT_ID}")
        database.expose_in_sqllab = True
        database.allow_ctas = False
        database.allow_cvas = False
        database.extra = json.dumps(
            {
                "metadata_params": {},
                "engine_params": {},
                "metadata_cache_timeout": {},
                "schemas_allowed_for_file_upload": [],
            }
        )
        database.created_by = database.changed_by = admin
        db.session.flush()

        dataset = (
            db.session.query(SqlaTable)
            .filter_by(database_id=database.id, schema=SCHEMA, table_name=TABLE)
            .one_or_none()
        )
        if dataset is None:
            dataset = SqlaTable(table_name=TABLE, schema=SCHEMA, database=database)
            db.session.add(dataset)
        dataset.database = database
        dataset.main_dttm_col = "full_date"
        dataset.filter_select_enabled = True
        dataset.created_by = dataset.changed_by = admin
        db.session.flush()

        try:
            dataset.fetch_metadata()
        except Exception as exc:
            print(f"Warning: could not refresh BigQuery metadata: {exc}")

        for column in dataset.columns:
            column.filterable = True
            column.groupby = True
            column.is_dttm = column.column_name == "full_date"
            column.created_by = column.changed_by = admin

        metric_defs = {
            "count_calls": ("COUNT(call_id)", "Total Calls", ",d"),
            "successful_sales": (
                "SUM(IF(has_successful_sale, 1, 0))",
                "Successful Sales",
                ",d",
            ),
            "success_rate_pct": (
                "100 * SAFE_DIVIDE(SUM(IF(has_successful_sale, 1, 0)), COUNT(call_id))",
                "Success Rate %",
                ".1f",
            ),
            "avg_talk_time_seconds": (
                "AVG(talk_time_seconds)",
                "Avg Talk Time Seconds",
                ",.0f",
            ),
        }
        for name, (expression, label, d3format) in metric_defs.items():
            upsert_metric(dataset, admin, name, expression, label, d3format)
        db.session.flush()

        datasource = f"{dataset.id}__table"
        base = {
            "datasource": datasource,
            "adhoc_filters": [],
            "time_range": "No filter",
            "url_params": {},
        }

        metrics = {
            name: metric_obj(name, expression, label)
            for name, (expression, label, _) in metric_defs.items()
        }

        chart_specs = [
            (
                "Total Calls",
                "big_number_total",
                {
                    **base,
                    "viz_type": "big_number_total",
                    "metric": metrics["count_calls"],
                    "granularity_sqla": "full_date",
                    "header_font_size": 0.42,
                    "subheader": "",
                    "subheader_font_size": 0.15,
                    "y_axis_format": ",d",
                    "queryFields": {"metric": "metrics"},
                },
            ),
            (
                "Successful Sales",
                "big_number_total",
                {
                    **base,
                    "viz_type": "big_number_total",
                    "metric": metrics["successful_sales"],
                    "granularity_sqla": "full_date",
                    "header_font_size": 0.42,
                    "subheader": "",
                    "subheader_font_size": 0.15,
                    "y_axis_format": ",d",
                    "queryFields": {"metric": "metrics"},
                },
            ),
            (
                "Success Rate",
                "big_number_total",
                {
                    **base,
                    "viz_type": "big_number_total",
                    "metric": metrics["success_rate_pct"],
                    "granularity_sqla": "full_date",
                    "header_font_size": 0.42,
                    "subheader": "% successful calls",
                    "subheader_font_size": 0.15,
                    "y_axis_format": ".1f",
                    "queryFields": {"metric": "metrics"},
                },
            ),
            (
                "Calls by Date",
                "echarts_timeseries_bar",
                {
                    **base,
                    "viz_type": "echarts_timeseries_bar",
                    "granularity_sqla": "full_date",
                    "time_grain_sqla": "P1D",
                    "metrics": [metrics["count_calls"]],
                    "groupby": [],
                    "row_limit": 10000,
                    "show_legend": False,
                    "show_brush": "auto",
                    "rich_tooltip": True,
                    "x_axis_label": "Date",
                    "y_axis_label": "Calls",
                    "y_axis_format": ",d",
                    "x_axis_format": "%Y-%m-%d",
                    "x_ticks_layout": "auto",
                    "color_scheme": "supersetColors",
                    "queryFields": {"metrics": "metrics", "groupby": "groupby"},
                },
            ),
            (
                "Outcome Breakdown",
                "pie",
                {
                    **base,
                    "viz_type": "pie",
                    "granularity_sqla": "full_date",
                    "groupby": ["outcome_category"],
                    "metric": metrics["count_calls"],
                    "donut": True,
                    "innerRadius": 42,
                    "outerRadius": 68,
                    "label_type": "key_percent",
                    "show_labels": True,
                    "labels_outside": True,
                    "label_line": True,
                    "show_legend": True,
                    "row_limit": 20,
                    "number_format": "SMART_NUMBER",
                    "color_scheme": "supersetColors",
                    "queryFields": {"metric": "metrics", "groupby": "groupby"},
                },
            ),
            (
                "Agent Performance",
                "table",
                {
                    **base,
                    "viz_type": "table",
                    "granularity_sqla": "full_date",
                    "groupby": ["agent_id"],
                    "metrics": [
                        metrics["count_calls"],
                        metrics["successful_sales"],
                        metrics["success_rate_pct"],
                        metrics["avg_talk_time_seconds"],
                    ],
                    "all_columns": [],
                    "percent_metrics": [],
                    "order_by_cols": [],
                    "row_limit": 50,
                    "server_page_length": 50,
                    "include_search": True,
                    "show_cell_bars": True,
                    "page_length": 50,
                    "queryFields": {"metrics": "metrics", "groupby": "groupby"},
                },
            ),
        ]

        charts = []
        for name, viz_type, params in chart_specs:
            chart = db.session.query(Slice).filter_by(slice_name=name).one_or_none()
            if chart is None:
                chart = Slice(slice_name=name)
                db.session.add(chart)
            chart.viz_type = viz_type
            chart.datasource_id = dataset.id
            chart.datasource_type = "table"
            chart.datasource_name = f"{SCHEMA}.{TABLE}"
            chart.params = json.dumps(params, sort_keys=True)
            chart.query_context = None
            chart.owners = [admin]
            chart.created_by = chart.changed_by = admin
            chart.last_saved_by = admin
            chart.last_saved_at = datetime.utcnow()
            charts.append(chart)
        db.session.flush()

        dashboard = (
            db.session.query(Dashboard)
            .filter_by(dashboard_title=DASHBOARD_TITLE)
            .one_or_none()
        )
        if dashboard is None:
            dashboard = Dashboard(dashboard_title=DASHBOARD_TITLE)
            db.session.add(dashboard)
        dashboard.slug = "telesales-bigquery-dashboard"
        dashboard.published = True
        dashboard.owners = [admin]
        dashboard.slices = charts
        dashboard.created_by = dashboard.changed_by = admin
        dashboard.json_metadata = json.dumps(
            {
                "timed_refresh_immune_slices": [],
                "expanded_slices": {},
                "refresh_frequency": 0,
                "default_filters": "{}",
                "color_namespace": "telesales_bigquery",
                "label_colors": {},
                "chart_configuration": {},
                "global_chart_configuration": {
                    "scope": {"rootPath": ["ROOT_ID"], "excluded": []}
                },
                "native_filter_configuration": [],
                "cross_filters_enabled": False,
            }
        )

        layout = {
            "DASHBOARD_VERSION_KEY": "v2",
            "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
            "GRID_ID": {
                "type": "GRID",
                "id": "GRID_ID",
                "parents": ["ROOT_ID"],
                "children": ["ROW-KPI", "ROW-MAIN", "ROW-TABLE"],
            },
            "HEADER_ID": {
                "type": "HEADER",
                "id": "HEADER_ID",
                "meta": {"text": DASHBOARD_TITLE},
            },
            "ROW-KPI": {
                "type": "ROW",
                "id": "ROW-KPI",
                "parents": ["ROOT_ID", "GRID_ID"],
                "children": [],
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
            },
            "ROW-MAIN": {
                "type": "ROW",
                "id": "ROW-MAIN",
                "parents": ["ROOT_ID", "GRID_ID"],
                "children": [],
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
            },
            "ROW-TABLE": {
                "type": "ROW",
                "id": "ROW-TABLE",
                "parents": ["ROOT_ID", "GRID_ID"],
                "children": [],
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
            },
        }
        for index in [0, 1, 2]:
            node_id, node = chart_node(
                charts[index], 4, 16, ["ROOT_ID", "GRID_ID", "ROW-KPI"]
            )
            layout[node_id] = node
            layout["ROW-KPI"]["children"].append(node_id)
        for index, width in [(3, 8), (4, 4)]:
            node_id, node = chart_node(
                charts[index], width, 50, ["ROOT_ID", "GRID_ID", "ROW-MAIN"]
            )
            layout[node_id] = node
            layout["ROW-MAIN"]["children"].append(node_id)
        node_id, node = chart_node(charts[5], 12, 50, ["ROOT_ID", "GRID_ID", "ROW-TABLE"])
        layout[node_id] = node
        layout["ROW-TABLE"]["children"].append(node_id)

        dashboard.position_json = json.dumps(layout, indent=2)
        db.session.commit()

        print(f"Seeded Superset BigQuery dashboard: /superset/dashboard/{dashboard.id}/")


if __name__ == "__main__":
    main()
