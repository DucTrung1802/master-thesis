import matplotlib.pyplot as plt
import numpy as np

# Generate 1000 data points
x = np.arange(1000)
bar_values = np.random.randint(10, 50, size=1000)  # random integer bar heights
line_values = np.random.randn(1000).cumsum()  # cumulative sum for smoother line

# Create figure and primary axis (for line)
with plt.style.context("seaborn"):
    fig, ax1 = plt.subplots(figsize=(12, 6))

    # Plot line chart on left y-axis
    ax1.plot(x, line_values, color="red", linewidth=2, label="Line Values")
    ax1.set_ylabel("Line Values (Left Y-Axis)", color="red")
    ax1.tick_params(axis="y", labelcolor="red")

    # Create secondary y-axis (for bar)
    ax2 = ax1.twinx()
    ax2.bar(x, bar_values, color="skyblue", alpha=1, label="Bar Values")
    ax2.set_ylabel("Bar Values (Right Y-Axis)", color="blue")
    ax2.tick_params(axis="y", labelcolor="blue")

    # Title
    plt.title("Line (Left Y-Axis) and Bar (Right Y-Axis) Chart with 1000 Points")

    # Show plot
    plt.show()
