import json
from pathlib import Path

INPUT_PATH = Path(r"src\model\lstm\lstm_vcb_1.ipynb")

# Same folder, same name, .txt extension
OUTPUT_PATH = INPUT_PATH.with_suffix(".txt")

# Read notebook00.
with open(INPUT_PATH, "r", encoding="utf-8") as f:
    notebook = json.load(f)

# Write txt
with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    for cell_num, cell in enumerate(notebook["cells"], 1):
        cell_type = cell.get("cell_type", "unknown")
        content = "".join(cell.get("source", []))

        f.write(f"Cell {cell_num} ({cell_type})\n")
        f.write(content)
        f.write("\n\n")

print(f"Done: {OUTPUT_PATH}")