# CCS Website Product Crawler

Automation utility that logs into the CCS website, discovers product models, downloads all required collateral, and organizes the files per customer naming rules.

## Features

- Automated login with provided credentials.
- Crawls every product in the Series catalog (configurable inclusion filters).
- Downloads General Catalog, PDF Dimension Drawing, DXF Drawing, STEP Drawing, Product Datasheet, and optional User Manual where available.
- Saves product image assets inside an `Images/` sub-folder.
- Normalizes filenames using suffix rules (_Manual, _Catalog, _Dimension, _DXF, _STEP, _Datasheet).
- Renames and repackages DXF/STEP assets as zipped archives.
- Deduplicates downloads and resumes safely if interrupted.
- Supports headless Chrome and local ChromeDriver path overrides.

## Prerequisites

- macOS (tested on 12.x) with Python 3.9+.
- Google Chrome version 114+.
- ChromeDriver matching the installed Chrome _or_ allow `webdriver-manager` to auto-install.
- Network access to <https://www.ccs-grp.com>.

## Installation

```bash
cd /Users/apple/Documents/Crwal
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

The main entry point is `ccs_crawler.py`. Chrome opens in a normal (non-headless) window so you can watch the automation; add `--headless` if you prefer to keep it hidden.

```bash
python3 ccs_crawler.py \
  --username "Samir@MachineVisionDirect.com" \
  --password "MVDLogin1!" \
  --output-root "/Users/apple/Documents/Crwal/output_clean" \
  --series-file "series_urls.txt"
```

### Optional Arguments

- `--series-url`: Restrict crawling to a single series page (`https://www.ccs-grp.com/products/series/1`). Can be supplied multiple times.
- `--series-file`: Path to a text/CSV file with series URLs (one per line).
- `--product-file`: Optional CSV with explicit product URLs or model numbers (see the sample `products.txt`).
- `--chromedriver`: Explicit path to ChromeDriver binary; otherwise `webdriver-manager` installs a matching build.
- `--headless`: Hide the Chrome window (useful for servers).
- `--max-products`: Stop after processing the specified number of products (useful for testing).
- `--overwrite`: Re-download even if a product directory already exists.
- `--sleep`: Base wait (seconds) between interactions.

### Sample configuration files

- `products.txt`: One model code per line for targeted runs.
- `series_urls.txt`: Default list of CCS series pages to crawl sequentially.

## Output Structure

```
output_root/
  └── LDR2-32RD2/
        ├── LDR2-32RD2_Catalog.pdf
        ├── LDR2-32RD2_Dimension.pdf
        ├── LDR2-32RD2_DXF.zip
        ├── LDR2-32RD2_Manual.pdf
        ├── LDR2-32RD2_STEP.zip
        ├── LDR2-32RD2_Datasheet.pdf
        └── Images/
             └── LDR2-32RD2.png
```

## Notes

- The script respects the CART workflow by adding the four required downloads before triggering a batch download. DXF and STEP assets are kept (or rebuilt) as zipped archives.
- When a STEP icon opens an external CAD portal (Cadenas / 3Dfindit), the crawler switches to the new tab, selects the `STEP AP214 (3D)` format on first visit, and triggers generation before downloading.
- PDF files are validated by attempting to open the first bytes to confirm `%PDF` magic; invalid downloads are retried.
- Existing downloads are skipped unless `--overwrite` is provided.

## Running Locally

1. Ensure VPN/proxy settings allow access to the CCS website.
2. Confirm the provided CCS credentials work manually once to avoid account lockouts.
3. Run the crawler with a small `--max-products` first to verify ChromeDriver and download folders.
4. Monitor the console logs; the script updates progress via `tqdm`.
5. After completion spot-check a handful of output folders (PDFs should open, zipped DXF/STEP should contain the expected files).

## Troubleshooting

- **Login failed**: Delete cookies (`--clear-cache` flag) or check credentials.
- **Downloads stuck**: Increase `--download-timeout` or update ChromeDriver.
- **Portal layout changes**: Adjust CSS selectors defined in `SELECTORS` at the top of `ccs_crawler.py`.
- **Headless issues**: Remove `--headless` to debug interactively.

## Legal

Before running a full crawl, confirm that automation complies with CCS website terms of service and any rate limits.

