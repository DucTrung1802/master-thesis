from PIL import Image
import os
import random

base_path = r"charts\macroeconomics"

# Get all PNG files in the folder
all_png_files = [f for f in os.listdir(base_path) if f.lower().endswith(".png")]

# Pick 5 random files
png_files = random.sample(all_png_files, 5)

# Build full paths
png_paths = [os.path.join(base_path, fname) for fname in png_files]

# Open and convert to RGB
image_list = [Image.open(png).convert("RGB") for png in png_paths]

# Save as PDF
output_path = os.path.join(base_path, "output.pdf")
image_list[0].save(output_path, save_all=True, append_images=image_list[1:])

print("Selected files:", png_files)
print(f"PDF saved at: {output_path}")
