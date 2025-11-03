import random
import os
import json
import wandb
from dotenv import load_dotenv
from datetime import datetime

# Load API key from .env
load_dotenv()
wandb.login(key=os.getenv("WANDB_KEY"))

# Start a new W&B run
run = wandb.init(
    entity="trung-lyduc18",
    project="master_thesis",
    config={
        "learning_rate": 0.02,
        "architecture": "CNN",
        "dataset": "CIFAR-100",
        "epochs": 10,
        "test": True,
    },
)

# --- Simulate training and collect metrics ---
epochs = 10
offset = random.random() / 5
metrics = []

for epoch in range(2, epochs):
    acc = 1 - 2**-epoch - random.random() / epoch - offset
    loss = 2**-epoch + random.random() / epoch + offset

    run.log({"epoch": epoch, "acc": acc, "loss": loss})
    metrics.append({"epoch": epoch, "acc": acc, "loss": loss})

# --- Save metrics locally before finishing run ---
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
filename = f"training_log_{timestamp}.json"

with open(filename, "w") as f:
    json.dump(metrics, f, indent=4)

# ✅ Upload the JSON file to W&B (run must still be active)
run.save(filename)

# Now finish the run
run.finish()

print(f"✅ Metrics saved and uploaded to W&B as {filename}")
