# SnowRunner Save Editor (Web Port)

This folder now includes a static web version of your editor:

- `index.html`
- `styles.css`
- `app.js`

## Run locally

Open `index.html` in a browser.

## Deploy to GitHub Pages

### Recommended: root user site

1. Create a public repo named exactly `mrboxik.github.io`.
2. Upload the contents of this folder into the root of that repo.
3. In GitHub: `Settings` -> `Pages`.
4. Under `Build and deployment`, set:
   - `Source`: `Deploy from a branch`
   - `Branch`: `main` (or your branch), folder: `/ (root)`
5. Save, wait for deployment, then open `https://mrboxik.github.io/`.

### Alternative: project site

1. Push these files to a normal GitHub repo.
2. In GitHub: `Settings` -> `Pages`.
3. Under `Build and deployment`, set:
   - `Source`: `Deploy from a branch`
   - `Branch`: `main` (or your branch), folder: `/ (root)`
4. Save, wait for deployment, then open the project Pages URL.

## What this web version supports

- Main save upload/edit/download:
  - Game Stats editor (`distance` + `gameStat`)
  - Money & Rank
  - Time
  - Objectives+ (search/filter/select and mark complete)
    - Uses embedded catalog data only (no runtime fetch required)
    - Search includes key/name/region/category/type fields from catalog data
  - Regions+ (desktop-aligned bulk region actions)
    - Upgrades
    - Watchtowers
    - Discoveries
    - Levels
    - Garages
    - Optional legacy Missions / Contests toggle
  - Rules (desktop-aligned New Game+ rule editor)
- CommonSslSave upload/edit/download:
  - Trials
  - PROS entitlements
  - Achievements unlock for existing entries

All edits are done in-browser. Files are not uploaded to a server. The folder loader also supports decoded WGS / Xbox App save folders and rebuilds their original layout on zip download.

## Objectives+ data notes

- Embedded catalog is bundled in `app.js`, so Objectives+ works even on `file://`.
- If you want frequent updates, regenerate `data/maprunner_data.csv` with your Python tool, then run `python data/embed_catalog_into_app.py` to re-embed it into `app.js`.
- Optional automation: `.github/workflows/update-objectives-catalog.yml` can run in GitHub Actions on schedule and auto-refresh `data/maprunner_data.csv` + embedded catalog in `app.js`.
