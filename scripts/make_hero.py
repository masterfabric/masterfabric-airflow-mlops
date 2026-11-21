"""Compose docs/hero.png (1600x900) from real screenshots and measured numbers.

Every number on the hero is read from docs/results/*.json at render time.
"""

from __future__ import annotations

import json

from PIL import Image, ImageDraw, ImageFilter

from mfviz import BG, BG2, DOCS, GREEN, GREY, ICE, IMG, ORANGE, PURPLE, RED, RESULTS, SHOTS, WHITE, YELLOW, font

W, H = 1600, 900


def load(n):
    return json.loads((RESULTS / f"{n}.json").read_text())


def background():
    im = Image.new("RGB", (W, H), BG)
    px = im.load()
    for y in range(H):
        for x in range(W):
            t = min(1.0, (x / W) * 0.75 + (y / H) * 0.35)
            px[x, y] = tuple(int(BG[i] * (1 - t) + BG2[i] * t) for i in range(3))
    return im


def card(base, path, box, label, color, crop=None):
    x0, y0, x1, y1 = box
    shot = Image.open(path).convert("RGB")
    if crop:
        shot = shot.crop(crop)
    shot = shot.resize((x1 - x0, round(shot.height * (x1 - x0) / shot.width)), Image.LANCZOS)
    shot = shot.crop((0, 0, x1 - x0, min(shot.height, y1 - y0)))
    shadow = Image.new("RGBA", (shot.width + 40, shot.height + 40), (0, 0, 0, 0))
    ImageDraw.Draw(shadow).rounded_rectangle((20, 20, shot.width + 20, shot.height + 20), radius=16, fill=(0, 0, 0, 170))
    shadow = shadow.filter(ImageFilter.GaussianBlur(12))
    base.paste(shadow, (x0 - 20, y0 - 12), shadow)
    mask = Image.new("L", shot.size, 0)
    ImageDraw.Draw(mask).rounded_rectangle((0, 0, shot.width, shot.height), radius=14, fill=255)
    base.paste(shot, (x0, y0), mask)
    d = ImageDraw.Draw(base)
    f = font(20, 800)
    w = d.textlength(label, font=f)
    d.rounded_rectangle((x0 - 10, y0 - 22, x0 + w + 18, y0 + 14), radius=18, fill=color)
    d.text((x0 + 4, y0 - 18), label, font=f, fill=(255, 255, 255))


def main():
    r = load("backfill")
    api, hs = load("api_demo"), load("hotswap_demo")
    real = {(x["month"], x["policy"]): x for x in r["realised"]}
    last = max(m for m, p in real if p == "auto")
    a, s = real[(last, "auto")], real[(last, "static")]
    dec = r["decisions"]
    promoted = [x for x in dec if x["action"] == "promoted"]
    rejected = [x for x in dec if x["action"] == "rejected"]
    halted = [x for x in dec if x["action"] == "halted"]
    im = background()
    card(im, SHOTS / "airflow_grid.png", (1000, 70, 1560, 320), "Airflow · churn_monthly", ICE, crop=(0, 0, 1440, 640))
    card(im, IMG / "realised_auc.png", (1010, 380, 1570, 610), "realised AUC: auto vs static", GREEN)
    card(im, SHOTS / "mlflow_registry_churn.png", (990, 670, 1510, 860), "MLflow registry · aliases", PURPLE,
         crop=(0, 260, 1440, 900))
    d = ImageDraw.Draw(im)
    d.text((80, 70), "MASTERFABRIC", font=font(36, 800), fill=WHITE)
    d.text((76, 120), "Airflow MLOps", font=font(92, 800), fill=WHITE)
    x, y = 80, 238
    for word, col in (("Airflow", ICE), (" · ", GREY), ("MLflow", PURPLE), (" · ", GREY), ("drift", ORANGE),
                      (" · ", GREY), ("challengers", GREEN)):
        f = font(40, 800)
        d.text((x, y), word, font=f, fill=col)
        x += d.textlength(word, font=f)
    d.text((80, 318), "12 monthly runs of a churn model: quality gate, PSI drift,",
           font=font(26, 500), fill=GREY)
    d.text((80, 352), "retraining, alias promotion, a hot-swapping API. All measured.", font=font(26, 500), fill=GREY)
    facts = [
        (f"realised AUC {a['auc']:.3f} vs {s['auc']:.3f} never-retrained ({last})",
         f"auto policy v{a['model_version']} vs static v{s['model_version']} · oracle upper bound {r['oracle_auc'][last]:.3f}", GREEN),
        (f"{len(promoted)} promoted · {len(rejected)} rejected · {len(halted)} month halted",
         f"{halted[0]['month']} feed failed {', '.join(halted[0]['reasons'][1:])}", ORANGE),
        (f"API p50 {api['single_request_ms']['p50']:.1f} ms · 1000 rows in {api['batch_1000_ms']:.0f} ms",
         f"max |API - batch| = {api['max_abs_diff_vs_batch_scoring']:.0e} · answered by v{api['answered_by_version']}", ICE),
        (f"rollback served in {hs['rollback']['seconds_until_served']:.1f} s, no restart",
         f"move @champion v{hs['start_version']} -> v{hs['rollback']['to']} -> v{hs['restore']['to']} · API polls every 15 s", PURPLE),
    ]
    y = 430
    for title, sub, col in facts:
        d.rounded_rectangle((80, y + 6, 88, y + 62), radius=4, fill=col)
        d.text((106, y), title, font=font(29, 750), fill=WHITE)
        d.text((106, y + 38), sub, font=font(20, 450), fill=GREY)
        y += 92
    d.text((80, 820), "github.com/masterfabric/masterfabric-airflow-mlops", font=font(30, 700), fill=WHITE)
    im.save(DOCS / "hero.png", optimize=True)
    print("docs/hero.png written")


if __name__ == "__main__":
    main()
