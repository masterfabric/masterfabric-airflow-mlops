"""Capture real UI screenshots of the running lab with Playwright (headless Chromium).

    make up && make backfill && make report   # first
    .venv/bin/python scripts/screenshots.py

Captures the Airflow grid and graph of churn_monthly, the MLflow registry and experiment, the scoring API's
Swagger page and the S3 console with the monthly partitions. Credentials are read from .env and typed into the
login forms; they are never printed.
"""
from __future__ import annotations

import sys
from pathlib import Path

from PIL import Image
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "screenshots"
ENV = dict(line.split("=", 1) for line in (ROOT / ".env").read_text().splitlines()
           if line and not line.startswith("#") and "=" in line)
AIRFLOW = f"http://127.0.0.1:{ENV.get('PORT_AIRFLOW', '38080')}"
MLFLOW = f"http://127.0.0.1:{ENV.get('PORT_MLFLOW', '35000')}"
API = f"http://127.0.0.1:{ENV.get('PORT_API', '38000')}"
S3 = f"http://127.0.0.1:{ENV.get('PORT_S3_CONSOLE', '39001')}"


def shot(page, name: str, full: bool = False) -> None:
    path = OUT / f"{name}.png"
    page.screenshot(path=str(path), full_page=full)
    im = Image.open(path)
    if im.width > 1440:
        im.resize((1440, round(im.height * 1440 / im.width)), Image.LANCZOS).save(path, optimize=True)
    print("saved", path.relative_to(ROOT))


def airflow(page) -> None:
    page.goto(f"{AIRFLOW}/login/")
    page.fill("#username", ENV["AIRFLOW_ADMIN_USER"])
    page.fill("#password", ENV["AIRFLOW_ADMIN_PASSWORD"])
    page.keyboard.press("Enter")
    page.wait_for_timeout(3000)
    page.goto(f"{AIRFLOW}/dags/churn_monthly/grid")
    page.wait_for_timeout(4000)
    shot(page, "airflow_grid")
    page.goto(f"{AIRFLOW}/dags/churn_monthly/grid?tab=graph&dag_run_id=scheduled__2026-08-01T00%3A00%3A00%2B00%3A00")
    page.wait_for_timeout(5000)
    shot(page, "airflow_graph_2026_08")


def mlflow(page) -> None:
    page.goto(f"{MLFLOW}/#/models/churn")
    page.wait_for_timeout(4000)
    close = page.locator(".ant-modal-close, [aria-label=Close]")   # one-time "new registry UI" announcement
    if close.count():
        close.first.click()
        page.wait_for_timeout(800)
    shot(page, "mlflow_registry_churn")
    page.goto(f"{MLFLOW}/#/experiments/1")
    page.wait_for_timeout(4500)
    shot(page, "mlflow_experiment_runs")


def api(page) -> None:
    page.goto(f"{API}/docs")
    page.wait_for_timeout(2500)
    page.get_by_text("/model", exact=True).first.click()
    page.wait_for_timeout(500)
    page.get_by_role("button", name="Try it out").first.click()
    page.get_by_role("button", name="Execute").first.click()
    page.wait_for_timeout(1500)
    page.locator(".responses-wrapper .live-responses-table").first.scroll_into_view_if_needed()
    shot(page, "api_swagger_model", full=True)


def s3(page) -> None:
    page.goto(f"{S3}/rustfs/console/auth/login/")
    page.fill("input[name=accessKey]", ENV["S3_ACCESS_KEY"])
    page.fill("input[name=secretKey]", ENV["S3_SECRET_KEY"])
    page.keyboard.press("Enter")
    page.wait_for_url("**/browser/**")
    page.wait_for_timeout(4000)
    page.goto(f"{S3}/rustfs/console/browser/?bucket=lake")
    page.wait_for_timeout(1500)
    for folder in ("scores/", "churn/"):
        page.get_by_text(folder, exact=True).first.click()
        page.wait_for_timeout(1500)
    shot(page, "s3_console_scores")


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    targets = sys.argv[1:] or ["airflow", "mlflow", "api", "s3"]
    with sync_playwright() as p:
        b = p.chromium.launch()
        page = b.new_page(viewport={"width": 1440, "height": 900}, device_scale_factor=2)
        for t in targets:
            {"airflow": airflow, "mlflow": mlflow, "api": api, "s3": s3}[t](page)
        b.close()
