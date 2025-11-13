#!/usr/bin/env python3
"""
Validate CCS crawler outputs:
- Ensure required files exist per product
- Verify PDFs have %PDF header
- Ensure DXF/STEP are zipped; clean zips to keep only target CAD files (remove README)
- Remove obvious duplicates (keep largest by size per suffix)
"""
from __future__ import annotations

import argparse
import os
import sys
import zipfile
from pathlib import Path
from typing import Dict, List, Tuple

PDF_MAGIC = b"%PDF"
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp"}

REQUIRED = {
    "_Catalog.pdf": True,
    "_Dimension.pdf": True,
    "_DXF.zip": True,
    "_STEP.zip": True,
    "_Manual.pdf": False,
    "_Datasheet.pdf": False,
}
OPTIONAL = {
    "_Manual.pdf": False,
    "_STEP.zip": False,
    "_Datasheet.pdf": False,
}


def is_pdf(path: Path) -> bool:
    try:
        with path.open("rb") as fh:
            head = fh.read(4)
        return head == PDF_MAGIC
    except Exception:
        return False


def collect_by_suffix(files: List[Path]) -> Dict[str, List[Path]]:
    buckets: Dict[str, List[Path]] = {}
    for p in files:
        for suffix in list(REQUIRED.keys()) + list(OPTIONAL.keys()):
            if p.name.endswith(suffix):
                buckets.setdefault(suffix, []).append(p)
                break
    return buckets


def remove_duplicates(buckets: Dict[str, List[Path]]) -> List[Tuple[str, List[Path]]]:
    removed: List[Tuple[str, List[Path]]] = []
    for suffix, paths in buckets.items():
        if len(paths) <= 1:
            continue
        # Keep the largest file; delete the others
        keep = max(paths, key=lambda p: p.stat().st_size if p.exists() else -1)
        to_remove = [p for p in paths if p != keep]
        for p in to_remove:
            try:
                p.unlink(missing_ok=True)
            except Exception:
                pass
        removed.append((suffix, to_remove))
    return removed


def clean_zip(zip_path: Path, mode: str) -> Tuple[bool, List[str]]:
    """
    mode: 'dxf' or 'step'
    Keep only files with matching CAD extension; drop README and others.
    """
    target_exts = {".dxf"} if mode == "dxf" else {".stp", ".step"}
    changed = False
    kept: List[str] = []
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            members = zf.namelist()
            # Filter to target CAD files
            target_members = [m for m in members if Path(m).suffix.lower() in target_exts]
            if not target_members:
                # Nothing to keep; leave as is
                return False, kept
            # If there are non-target files, rebuild
            if set(target_members) != set(members):
                tmp_zip = zip_path.with_suffix(".tmp.zip")
                with zipfile.ZipFile(tmp_zip, "w", compression=zipfile.ZIP_DEFLATED) as outz:
                    for m in target_members:
                        data = zf.read(m)
                        arcname = Path(m).name  # flatten paths
                        outz.writestr(arcname, data)
                        kept.append(arcname)
                tmp_zip.replace(zip_path)
                changed = True
            else:
                kept = [Path(m).name for m in target_members]
    except zipfile.BadZipFile:
        return False, kept
    return changed, kept


def validate_product_dir(prod_dir: Path) -> Dict[str, object]:
    result = {
        "product": prod_dir.name,
        "status": "ok",
        "issues": [],
        "fixed": [],
    }
    files = [p for p in prod_dir.glob("*") if p.is_file()]
    buckets = collect_by_suffix(files)

    # Remove duplicates
    dup_removed = remove_duplicates(buckets)
    if dup_removed:
        result["fixed"].append(f"Removed duplicates: {', '.join(s for s,_ in dup_removed)}")

    # Required checks
    for suffix, required in REQUIRED.items():
        present = any(p.name.endswith(suffix) for p in prod_dir.glob(f"*{suffix}"))
        if required and not present:
            result["status"] = "warn"
            result["issues"].append(f"Missing required {suffix}")

    # PDF validity
    for pdf_suffix in ("_Catalog.pdf", "_Dimension.pdf", "_Manual.pdf", "_Datasheet.pdf"):
        for pdf in prod_dir.glob(f"*{pdf_suffix}"):
            if pdf.stat().st_size == 0 or not is_pdf(pdf):
                result["status"] = "warn"
                result["issues"].append(f"Invalid PDF: {pdf.name}")
                try:
                    pdf.unlink(missing_ok=True)
                    result["fixed"].append(f"Deleted invalid PDF: {pdf.name}")
                except Exception:
                    pass

    # DXF zip cleanup
    for z in prod_dir.glob("*_DXF.zip"):
        changed, kept = clean_zip(z, "dxf")
        if changed:
            result["fixed"].append(f"Cleaned DXF zip: kept {kept}")
        # ensure at least one .dxf inside
        try:
            with zipfile.ZipFile(z, "r") as zf:
                if not any(Path(n).suffix.lower() == ".dxf" for n in zf.namelist()):
                    result["status"] = "warn"
                    result["issues"].append(f"DXF zip missing .dxf: {z.name}")
        except zipfile.BadZipFile:
            result["status"] = "warn"
            result["issues"].append(f"Corrupt DXF zip: {z.name}")

    # STEP zip cleanup
    for z in prod_dir.glob("*_STEP.zip"):
        changed, kept = clean_zip(z, "step")
        if changed:
            result["fixed"].append(f"Cleaned STEP zip: kept {kept}")
        try:
            with zipfile.ZipFile(z, "r") as zf:
                if not any(Path(n).suffix.lower() in {".stp", ".step"} for n in zf.namelist()):
                    result["status"] = "warn"
                    result["issues"].append(f"STEP zip missing .stp/.step: {z.name}")
        except zipfile.BadZipFile:
            result["status"] = "warn"
            result["issues"].append(f"Corrupt STEP zip: {z.name}")

    # Images folder
    images_ok = False
    images_dir = prod_dir / "Images"
    if images_dir.exists() and images_dir.is_dir():
        for img in images_dir.iterdir():
            if img.is_file() and img.suffix.lower() in IMAGE_EXTS and img.stat().st_size > 0:
                images_ok = True
                break
    if not images_ok:
        result["status"] = "warn"
        result["issues"].append("Missing or invalid product image in Images/")

    return result


def validate_roots(roots: List[Path]) -> List[Dict[str, object]]:
    all_results: List[Dict[str, object]] = []
    for root in roots:
        if not root.exists():
            continue
        for prod_dir in sorted([p for p in root.iterdir() if p.is_dir()]):
            # Only validate folders marked complete by crawler
            if not (prod_dir / ".complete").exists():
                continue
            res = validate_product_dir(prod_dir)
            all_results.append(res)
    return all_results


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--roots", nargs="+", required=True, help="Output root folders to validate")
    args = parser.parse_args(argv)
    roots = [Path(p).expanduser().resolve() for p in args.roots]
    results = validate_roots(roots)
    ok = sum(1 for r in results if r["status"] == "ok")
    warn = len(results) - ok
    print(f"Validated {len(results)} product folders: OK={ok}, WARN={warn}")
    for r in results:
        if r["status"] != "ok":
            print(f"- {r['product']}: WARN")
            for issue in r["issues"]:
                print(f"  * {issue}")
        for fix in r["fixed"]:
            print(f"  fixed: {fix}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))


