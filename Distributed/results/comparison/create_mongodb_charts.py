from pathlib import Path
import csv
import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt

# Define input and output paths.
folder = Path(__file__).resolve().parent
source = folder / "performance_comparison.csv"
output = folder / "matplotlib_charts"

output.mkdir(parents=True, exist_ok=True)

if not source.exists():
    raise SystemExit("Performance comparison CSV not found.")

# Read the original benchmark results.
results = {}

with source.open(newline="") as file:
    reader = csv.DictReader(file)

    for row in reader:
        query = row["Query"]
        dataset = row["Dataset"]
        key = (query, dataset)

        if key in results:
            raise ValueError(f"Duplicate result: {key}")

        results[key] = row

# Verify all query and dataset combinations.
datasets = ["100K", "500K", "1M"]

for q in range(1, 6):
    for dataset in datasets:
        if (f"Q{q}", dataset) not in results:
            raise ValueError(f"Missing Q{q} {dataset}")

# Create comparison charts.
for q in range(1, 6):
    query = f"Q{q}"

    for metric in ["Median", "P95"]:

        central_key = f"Centralized {metric} (ms)"
        distributed_key = f"Distributed {metric} (ms)"

        central = [
            float(results[(query, size)][central_key])
            for size in datasets
        ]

        distributed = [
            float(results[(query, size)][distributed_key])
            for size in datasets
        ]

        # Plot grouped bars for both systems.
        x = range(len(datasets))

        left = [i - 0.2 for i in x]
        right = [i + 0.2 for i in x]

        fig, ax = plt.subplots(figsize=(9, 5))

        bars1 = ax.bar(
            left,
            central,
            width=0.4,
            label="Centralized"
        )

        bars2 = ax.bar(
            right,
            distributed,
            width=0.4,
            label="Distributed"
        )

        # Add chart titles and labels.
        ax.set_title(f"{query} {metric} Latency Comparison")
        ax.set_xlabel("Dataset Size")
        ax.set_ylabel("Latency (ms)")

        ax.set_xticks(list(x))
        ax.set_xticklabels(datasets)

        ax.legend()
        ax.grid(axis="y", alpha=0.25)
        ax.set_axisbelow(True)

        # Display latency values above the bars.
        ax.set_ylim(
            0,
            max(central + distributed) * 1.2
        )

        ax.bar_label(bars1, fmt="%g", padding=3)
        ax.bar_label(bars2, fmt="%g", padding=3)

        # Save the chart as a PNG image.
        filename = output / f"{query}_{metric}.png"

        fig.tight_layout()
        fig.savefig(filename, dpi=250)
        plt.close(fig)

        print("Created:", filename.name)

print("Total charts: 10")
print("Saved in:", output)
