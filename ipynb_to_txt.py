import json
from pathlib import Path

# List of notebook paths
INPUT_PATHS = [
    Path(r"src\train_test_creator\train_test_creator.ipynb"),
    Path(r"src\evaluator\data_evaluator.ipynb"),
    Path(r"src\model\lstm\lstm_vcb_1.ipynb"),
]

for input_path in INPUT_PATHS:
    output_path = input_path.with_suffix(".txt")

    # Read notebook
    with open(input_path, "r", encoding="utf-8") as f:
        notebook = json.load(f)

    # Write txt
    with open(output_path, "w", encoding="utf-8") as f:
        for cell_num, cell in enumerate(notebook["cells"], 1):
            cell_type = cell.get("cell_type", "unknown")
            content = "".join(cell.get("source", []))

            f.write(f"Cell {cell_num} ({cell_type})\n")
            f.write(content)
            f.write("\n\n")

    print(f"Done: {output_path}")