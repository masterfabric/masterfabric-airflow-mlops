"""Screenshots for the Evidently / Feast / canary addition (Playwright, headless Chromium).

    make up-monitoring && scripts/backfill_monitoring.sh && .venv/bin/python scripts/screenshots_monitoring.py

Credentials are read from .env and typed into the Airflow login form; they are never printed.
"""
from __future__ import annotations

import sys

from playwright.sync_api import sync_playwright

sys.path.insert(0, __file__.rsplit("/", 1)[0])
from screenshots import AIRFLOW, ENV, MLFLOW, shot  # noqa: E402

EVIDENTLY = f"http://127.0.0.1:{ENV.get('PORT_EVIDENTLY_UI', '38085')}"
FEAST = f"http://127.0.0.1:{ENV.get('PORT_FEAST_UI', '38888')}"


def evidently(page) -> None:
    page.goto(EVIDENTLY)
    page.wait_for_timeout(2500)
    page.get_by_text("churn-monitoring").first.click()
    page.wait_for_timeout(5000)
    shot(page, "evidently_dashboard", full=True)
    page.get_by_role("tab", name="Reports").first.click() if page.get_by_role("tab", name="Reports").count() else \
        page.get_by_text("Reports", exact=True).first.click()
    page.wait_for_timeout(3000)
    shot(page, "evidently_reports")
    row = page.locator("tr", has_text="drift 2026-07")
    row.get_by_text("View", exact=True).first.click()
    page.wait_for_timeout(7000)
    shot(page, "evidently_drift_2026_07")


def feast(page) -> None:
    page.goto(f"{FEAST}/p/mf_churn/feature-view/subscriber_monthly")
    page.wait_for_timeout(5000)
    shot(page, "feast_feature_view")
    page.goto(f"{FEAST}/p/mf_churn/lineage")
    page.wait_for_timeout(5000)
    shot(page, "feast_lineage")


def mlflow(page) -> None:
    page.goto(f"{MLFLOW}/#/models/churn")
    page.wait_for_timeout(4000)
    close = page.locator(".ant-modal-close, [aria-label=Close]")
    if close.count():
        close.first.click()
        page.wait_for_timeout(800)
    shot(page, "mlflow_registry_after_rollout")
    page.goto(f"{MLFLOW}/#/models/churn/versions/6")
    page.wait_for_timeout(4000)
    shot(page, "mlflow_v6_rollback_tag", full=True)


def airflow(page) -> None:
    page.goto(f"{AIRFLOW}/login/")
    page.fill("#username", ENV["AIRFLOW_ADMIN_USER"])
    page.fill("#password", ENV["AIRFLOW_ADMIN_PASSWORD"])
    page.keyboard.press("Enter")
    page.wait_for_timeout(3000)
    page.goto(f"{AIRFLOW}/dags/churn_monthly/grid?tab=graph")
    page.wait_for_timeout(5000)
    shot(page, "airflow_graph_monitoring_tasks")


def main() -> None:
    only = set(sys.argv[1:])
    with sync_playwright() as p:
        b = p.chromium.launch()
        page = b.new_page(viewport={"width": 1440, "height": 900}, device_scale_factor=2)
        for f in (evidently, feast, mlflow, airflow):
            if not only or f.__name__ in only:
                f(page)
        b.close()


if __name__ == "__main__":
    main()
