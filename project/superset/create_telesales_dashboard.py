import json
from datetime import datetime

from superset.app import create_app
from superset.extensions import db


PROJECT_ID = "project-ef0c6db5-0765-4391-845"
SCHEMA = "kltn0710"
DB_NAME = "BigQuery - KLTN Telesales"
DASHBOARD_TITLE = "KLTN Hybrid Lakehouse - End-to-End BI Dashboard"
LEGACY_DASHBOARD_TITLES = ["Telesales BigQuery Dashboard"]


DATASETS = {
    "primary": {
        "table": "vw_telesales_performance",
        "dttm": "full_date",
        "metrics": {
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
        },
    },
    "callcenter": {
        "table": "vw_callcenteren_performance",
        "dttm": None,
        "metrics": {
            "count_callcenter_calls": (
                "COUNT(callcenter_call_id)",
                "CallCenterEN Calls",
                ",d",
            ),
            "avg_audio_duration": (
                "AVG(audio_duration)",
                "Avg Audio Duration",
                ",.0f",
            ),
            "avg_model_confidence": (
                "AVG(model_call_code_confidence)",
                "Avg Model Confidence",
                ".3f",
            ),
            "training_ready_calls": (
                "SUM(IF(should_use_for_training, 1, 0))",
                "Training-Ready Calls",
                ",d",
            ),
        },
    },
    "callcenter_codes": {
        "table": "vw_callcenteren_call_codes",
        "dttm": None,
        "metrics": {
            "count_label_links": (
                "COUNT(callcenter_call_id)",
                "Call-Code Links",
                ",d",
            ),
            "avg_label_confidence": (
                "AVG(label_confidence)",
                "Avg Label Confidence",
                ".3f",
            ),
        },
    },
    "dataset_profile": {
        "table": "dataset_profile_comparison",
        "dttm": None,
        "metrics": {
            "rows": ("SUM(row_count)", "Rows", ",d"),
            "avg_duration": ("AVG(avg_duration_seconds)", "Avg Duration Seconds", ",.0f"),
            "avg_words": ("AVG(avg_word_count)", "Avg Word Count", ",.0f"),
            "avg_pii": ("AVG(avg_pii_token_count)", "Avg PII Tokens", ",.1f"),
        },
    },
    "label_distribution": {
        "table": "call_code_distribution_comparison",
        "dttm": None,
        "metrics": {
            "label_count": ("SUM(label_count)", "Label Count", ",d"),
        },
    },
    "model_experiment": {
        "table": "model_experiment_comparison",
        "dttm": None,
        "metrics": {
            "eval_rows": ("SUM(eval_rows)", "Eval Rows", ",d"),
            "micro_f1": ("AVG(micro_f1)", "Micro-F1", ".3f"),
            "exact_match_rate": ("AVG(exact_match_rate)", "Exact Match Rate", ".3f"),
        },
    },
}


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


def get_or_create_database(admin):
    from superset.models.core import Database

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
    return database


def get_or_create_dataset(database, admin, table_name, main_dttm_col):
    from superset.connectors.sqla.models import SqlaTable

    dataset = (
        db.session.query(SqlaTable)
        .filter_by(database_id=database.id, schema=SCHEMA, table_name=table_name)
        .one_or_none()
    )
    if dataset is None:
        dataset = SqlaTable(table_name=table_name, schema=SCHEMA, database=database)
        db.session.add(dataset)
    dataset.database = database
    dataset.main_dttm_col = main_dttm_col
    dataset.filter_select_enabled = True
    dataset.created_by = dataset.changed_by = admin
    db.session.flush()

    try:
        dataset.fetch_metadata()
    except Exception as exc:
        print(f"Warning: could not refresh metadata for {table_name}: {exc}")

    for column in dataset.columns:
        column.filterable = True
        column.groupby = True
        column.is_dttm = bool(main_dttm_col and column.column_name == main_dttm_col)
        column.created_by = column.changed_by = admin
    return dataset


def chart_node(chart, width, height, parents):
    node_id = f"CHART-{chart.id}"
    return node_id, {
        "children": [],
        "id": node_id,
        "meta": {"chartId": chart.id, "height": height, "width": width},
        "parents": parents,
        "type": "CHART",
    }


def make_base(dataset):
    return {
        "datasource": f"{dataset.id}__table",
        "adhoc_filters": [],
        "time_range": "No filter",
        "url_params": {},
    }


def big_number(base, metric, subheader="", y_axis_format=",d"):
    return {
        **base,
        "viz_type": "big_number_total",
        "metric": metric,
        "header_font_size": 0.4,
        "subheader": subheader,
        "subheader_font_size": 0.14,
        "y_axis_format": y_axis_format,
        "queryFields": {"metric": "metrics"},
    }


def pie_chart(base, metric, groupby, row_limit=20):
    return {
        **base,
        "viz_type": "pie",
        "groupby": [groupby],
        "metric": metric,
        "donut": True,
        "innerRadius": 42,
        "outerRadius": 68,
        "label_type": "key_percent",
        "show_labels": True,
        "labels_outside": True,
        "label_line": True,
        "show_legend": True,
        "row_limit": row_limit,
        "number_format": "SMART_NUMBER",
        "color_scheme": "supersetColors",
        "queryFields": {"metric": "metrics", "groupby": "groupby"},
    }


def time_bar(base, metric):
    return {
        **base,
        "viz_type": "echarts_timeseries_bar",
        "granularity_sqla": "full_date",
        "time_grain_sqla": "P1D",
        "metrics": [metric],
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
    }


def table_chart(base, groupby, metrics, row_limit=50, order_by_cols=None):
    return {
        **base,
        "viz_type": "table",
        "groupby": groupby,
        "metrics": metrics,
        "all_columns": [],
        "percent_metrics": [],
        "order_by_cols": order_by_cols or [],
        "row_limit": row_limit,
        "server_page_length": row_limit,
        "include_search": True,
        "show_cell_bars": True,
        "page_length": row_limit,
        "queryFields": {"metrics": "metrics", "groupby": "groupby"},
    }


def upsert_chart(admin, dataset, name, viz_type, params):
    from superset.models.slice import Slice

    chart = db.session.query(Slice).filter_by(slice_name=name).one_or_none()
    if chart is None:
        chart = Slice(slice_name=name)
        db.session.add(chart)
    chart.viz_type = viz_type
    chart.datasource_id = dataset.id
    chart.datasource_type = "table"
    chart.datasource_name = f"{SCHEMA}.{dataset.table_name}"
    chart.params = json.dumps(params, sort_keys=True)
    chart.query_context = None
    chart.owners = [admin]
    chart.created_by = chart.changed_by = admin
    chart.last_saved_by = admin
    chart.last_saved_at = datetime.utcnow()
    return chart


def add_row(layout, row_id):
    layout["GRID_ID"]["children"].append(row_id)
    layout[row_id] = {
        "type": "ROW",
        "id": row_id,
        "parents": ["ROOT_ID", "GRID_ID"],
        "children": [],
        "meta": {"background": "BACKGROUND_TRANSPARENT"},
    }


def add_chart_to_row(layout, row_id, chart, width, height):
    node_id, node = chart_node(chart, width, height, ["ROOT_ID", "GRID_ID", row_id])
    layout[node_id] = node
    layout[row_id]["children"].append(node_id)


def main():
    app = create_app()
    with app.app_context():
        from superset.models.dashboard import Dashboard

        admin = app.appbuilder.sm.find_user(username="admin")
        database = get_or_create_database(admin)

        datasets = {}
        metrics = {}
        for key, config in DATASETS.items():
            dataset = get_or_create_dataset(
                database,
                admin,
                config["table"],
                config["dttm"],
            )
            datasets[key] = dataset
            metrics[key] = {}
            for name, (expression, label, d3format) in config["metrics"].items():
                upsert_metric(dataset, admin, name, expression, label, d3format)
                metrics[key][name] = metric_obj(name, expression, label)
        db.session.flush()

        primary_base = make_base(datasets["primary"])
        callcenter_base = make_base(datasets["callcenter"])
        callcenter_codes_base = make_base(datasets["callcenter_codes"])
        dataset_profile_base = make_base(datasets["dataset_profile"])
        label_distribution_base = make_base(datasets["label_distribution"])
        model_experiment_base = make_base(datasets["model_experiment"])

        chart_specs = [
            (
                "KLTN - Primary Total Calls",
                datasets["primary"],
                "big_number_total",
                big_number(primary_base, metrics["primary"]["count_calls"]),
            ),
            (
                "KLTN - Primary Successful Sales",
                datasets["primary"],
                "big_number_total",
                big_number(primary_base, metrics["primary"]["successful_sales"]),
            ),
            (
                "KLTN - Primary Success Rate",
                datasets["primary"],
                "big_number_total",
                big_number(
                    primary_base,
                    metrics["primary"]["success_rate_pct"],
                    "successful calls",
                    ".1f",
                ),
            ),
            (
                "KLTN - CallCenterEN Calls",
                datasets["callcenter"],
                "big_number_total",
                big_number(callcenter_base, metrics["callcenter"]["count_callcenter_calls"]),
            ),
            (
                "KLTN - CallCenterEN Label Links",
                datasets["callcenter_codes"],
                "big_number_total",
                big_number(
                    callcenter_codes_base,
                    metrics["callcenter_codes"]["count_label_links"],
                    "bridge rows",
                ),
            ),
            (
                "KLTN - CallCenterEN Avg Model Confidence",
                datasets["callcenter"],
                "big_number_total",
                big_number(
                    callcenter_base,
                    metrics["callcenter"]["avg_model_confidence"],
                    "model_call_code confidence",
                    ".3f",
                ),
            ),
            (
                "KLTN - Primary Calls by Date",
                datasets["primary"],
                "echarts_timeseries_bar",
                time_bar(primary_base, metrics["primary"]["count_calls"]),
            ),
            (
                "KLTN - Primary Outcome Breakdown",
                datasets["primary"],
                "pie",
                pie_chart(primary_base, metrics["primary"]["count_calls"], "outcome_category"),
            ),
            (
                "KLTN - Primary Product Category",
                datasets["primary"],
                "pie",
                pie_chart(primary_base, metrics["primary"]["count_calls"], "product_category"),
            ),
            (
                "KLTN - Primary Agent Performance",
                datasets["primary"],
                "table",
                table_chart(
                    primary_base,
                    ["agent_id"],
                    [
                        metrics["primary"]["count_calls"],
                        metrics["primary"]["successful_sales"],
                        metrics["primary"]["success_rate_pct"],
                        metrics["primary"]["avg_talk_time_seconds"],
                    ],
                    row_limit=50,
                ),
            ),
            (
                "KLTN - CallCenterEN Source Domains",
                datasets["callcenter"],
                "pie",
                pie_chart(
                    callcenter_base,
                    metrics["callcenter"]["count_callcenter_calls"],
                    "source_domain",
                    row_limit=12,
                ),
            ),
            (
                "KLTN - CallCenterEN Direction Mix",
                datasets["callcenter"],
                "pie",
                pie_chart(
                    callcenter_base,
                    metrics["callcenter"]["count_callcenter_calls"],
                    "call_direction",
                    row_limit=8,
                ),
            ),
            (
                "KLTN - CallCenterEN Top Call Codes",
                datasets["callcenter_codes"],
                "pie",
                pie_chart(
                    callcenter_codes_base,
                    metrics["callcenter_codes"]["count_label_links"],
                    "call_code",
                    row_limit=15,
                ),
            ),
            (
                "KLTN - Multi-Source Dataset Profile",
                datasets["dataset_profile"],
                "table",
                table_chart(
                    dataset_profile_base,
                    ["dataset_name"],
                    [
                        metrics["dataset_profile"]["rows"],
                        metrics["dataset_profile"]["avg_duration"],
                        metrics["dataset_profile"]["avg_words"],
                        metrics["dataset_profile"]["avg_pii"],
                    ],
                    row_limit=10,
                ),
            ),
            (
                "KLTN - Multi-Source Call Code Distribution",
                datasets["label_distribution"],
                "table",
                table_chart(
                    label_distribution_base,
                    ["dataset_name", "call_code"],
                    [metrics["label_distribution"]["label_count"]],
                    row_limit=60,
                ),
            ),
            (
                "KLTN - Model Experiment Comparison",
                datasets["model_experiment"],
                "table",
                table_chart(
                    model_experiment_base,
                    ["model", "train_dataset", "eval_dataset"],
                    [
                        metrics["model_experiment"]["eval_rows"],
                        metrics["model_experiment"]["micro_f1"],
                        metrics["model_experiment"]["exact_match_rate"],
                    ],
                    row_limit=20,
                ),
            ),
        ]

        charts = [
            upsert_chart(admin, dataset, name, viz_type, params)
            for name, dataset, viz_type, params in chart_specs
        ]
        db.session.flush()

        dashboard = (
            db.session.query(Dashboard)
            .filter_by(dashboard_title=DASHBOARD_TITLE)
            .one_or_none()
        )
        if dashboard is None:
            dashboard = Dashboard(dashboard_title=DASHBOARD_TITLE)
            db.session.add(dashboard)
        dashboard.slug = "kltn-hybrid-lakehouse-end-to-end-bi"
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
                "color_namespace": "kltn_hybrid_lakehouse",
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
                "children": [],
            },
        }
        for row_id in [
            "ROW-PRIMARY-KPI",
            "ROW-CALLCENTER-KPI",
            "ROW-PRIMARY-CHARTS",
            "ROW-PRIMARY-TABLE",
            "ROW-CALLCENTER-CHARTS",
            "ROW-COMPARISON",
        ]:
            add_row(layout, row_id)

        for index in [0, 1, 2]:
            add_chart_to_row(layout, "ROW-PRIMARY-KPI", charts[index], 4, 16)
        for index in [3, 4, 5]:
            add_chart_to_row(layout, "ROW-CALLCENTER-KPI", charts[index], 4, 16)
        for index, width in [(6, 5), (7, 4), (8, 3)]:
            add_chart_to_row(layout, "ROW-PRIMARY-CHARTS", charts[index], width, 48)
        add_chart_to_row(layout, "ROW-PRIMARY-TABLE", charts[9], 12, 48)
        for index in [10, 11, 12]:
            add_chart_to_row(layout, "ROW-CALLCENTER-CHARTS", charts[index], 4, 46)
        for index in [13, 14, 15]:
            add_chart_to_row(layout, "ROW-COMPARISON", charts[index], 4, 52)

        dashboard.position_json = json.dumps(layout, indent=2)

        for legacy_title in LEGACY_DASHBOARD_TITLES:
            legacy_dashboard = (
                db.session.query(Dashboard)
                .filter_by(dashboard_title=legacy_title)
                .one_or_none()
            )
            if legacy_dashboard is not None:
                legacy_dashboard.published = False
                legacy_dashboard.changed_by = admin

        db.session.commit()

        print(f"Seeded Superset dashboard: /superset/dashboard/{dashboard.id}/")


if __name__ == "__main__":
    main()
