# Phase 1 architecture diagram generator

`gen_phase1_arch.py` builds `docs/architecture/phase1_ingestion_arch.drawio` (5 pages: Architecture,
Design Patterns, State Machines, Use Cases, Deployment). Edit the diagram by editing this script, not the
`.drawio` file, so the layout checks keep working.

## Regenerate

From the repo root:

```bash
python docs/architecture/generator/gen_phase1_arch.py docs/architecture/phase1_ingestion_arch.drawio docs/architecture/generator/measured.json
```

The script exits non-zero and lists the problem if any page has overlapping boxes, a box outside
its lane/frame, an edge running through a box, or two edges crossing.

## Files

| File | Purpose |
|---|---|
| `gen_phase1_arch.py` | Generator. Standard-library Python only. |
| `measured.json` | Rendered text height (px) per box, keyed `page:box`. Box heights = measured + padding. |
| `measure.html` | Browser page that renders a `.drawio` file with the draw.io viewer and returns `measure()` (text heights) and `audit()` (text overflow, label overlaps, excess empty space). |

## After changing label text

Box heights come from `measured.json`, so changed text needs re-measuring:

1. Build to a scratch file: `python docs/architecture/generator/gen_phase1_arch.py /tmp/scratch.drawio docs/architecture/generator/measured.json`
2. Serve the folder containing `measure.html` and the scratch file over HTTP, e.g. `python -m http.server 8765 --bind 127.0.0.1`
3. Open `http://127.0.0.1:8765/measure.html` and run `JSON.stringify(await measure('scratch.drawio'))` in the browser console.
4. Copy the entries you changed into `measured.json`, rebuild, then run `await audit('scratch.drawio')`: every page's `problems` list should be empty.

`measure.html` loads `viewer-static.min.js` from viewer.diagrams.net; the diagram itself stays local.

## Conventions

- Element kind → tag colour is defined once in `KINDS` and explained in each page's legend.
- `tag(kind, pattern, "Pn")` adds the coloured kind tag and the design-pattern stereotype; `Pn` refers to
  frame Pn on the Design Patterns page.
- Pass `tooltip=` to `vertex()` for hover detail; lane / frame comments are `note` boxes.
- Dashed, unfilled group outlines (the EventBus around the four topics, the Rule plugins group in the
  Oversee lane, the compute node on the Deployment page) are created with `check=False`; the overlap checks and `audit()` ignore them because
  other boxes and labels sit inside them on purpose.
- Names such as `AdapterFactory`, `ReadOnlyTransport`, `OrderUnitOfWork` and the DLQ status values are
  proposals; rename them here once the real code exists.
