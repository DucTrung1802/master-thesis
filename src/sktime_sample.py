import matplotlib.pyplot as plt
from sktime.transformations.series.feature_selection import FeatureSelection
from sktime.datasets import load_longley
import pandas as pd
import seaborn as sns
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
import numpy as np

# -------------------------
# Load dataset
# -------------------------
y, X = load_longley()

# -------------------------
# Handle multicollinearity
# -------------------------
corr_matrix = X.corr().abs()
high_corr = set()
threshold = 0.8
for i in range(len(corr_matrix.columns)):
    for j in range(i):
        if corr_matrix.iloc[i, j] > threshold:
            high_corr.add(corr_matrix.columns[i])

if high_corr:
    correlated_features = list(high_corr)
    print("Highly correlated features:", correlated_features)

    # Standardize features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X[correlated_features])

    # Apply PCA automatically to explain 95% variance
    pca = PCA(n_components=0.95, random_state=42)
    X_pca = pca.fit_transform(X_scaled)
    n_pca = X_pca.shape[1]
    print(f"PCA automatically selected {n_pca} components to explain 95% variance.")

    # Create new DataFrame with PCA components
    X_new = X.drop(columns=correlated_features)
    for i in range(n_pca):
        X_new[f"PCA_combined_{i+1}"] = X_pca[:, i]
else:
    X_new = X.copy()
    n_pca = 0

# -------------------------
# Feature selection
# -------------------------
transformer = FeatureSelection(
    method="feature-importances", n_columns=3, random_state=42
)
Xt = transformer.fit_transform(X_new, y)

importances_dict = transformer.feature_importances_
importances = pd.Series(importances_dict)
top_features = importances.sort_values(ascending=False).head(3).index.tolist()
print("Top features selected:", top_features)

# -------------------------
# Recommendation table
# -------------------------
feature_info = []
for col in X_new.columns:
    role = "PCA_combined" if col.startswith("PCA_combined") else "Original Feature"
    feature_info.append(
        {
            "Feature": col,
            "Role": role,
            "Importance": importances[col],
            "Top Selected": "Yes" if col in top_features else "No",
        }
    )

recommendation_df = pd.DataFrame(feature_info)
recommendation_df = recommendation_df.sort_values(
    by="Importance", ascending=False
).reset_index(drop=True)
print(recommendation_df)

# -------------------------
# Plot series diagram
# -------------------------
all_series = pd.concat([y, X_new], axis=1)
plot_index = range(len(all_series))
n_series = all_series.shape[1]

fig, axes = plt.subplots(n_series, 1, figsize=(14, 1.5 * n_series), sharex=True)

for i, col in enumerate(all_series.columns):
    if col in top_features:
        axes[i].plot(plot_index, all_series[col], marker="o", color="red")
        title_extra = f"Feature - Importance: {importances[col]:.2f} (TOP)"
    elif col == "TOTEMP":
        axes[i].plot(plot_index, all_series[col], marker="o", color="blue")
        title_extra = "Target (y)"
    else:
        axes[i].plot(plot_index, all_series[col], marker="o", color="gray")
        title_extra = "Feature"

    axes[i].set_title(f"{col} → {title_extra}", fontsize=16)
    axes[i].grid(True)

plt.tight_layout()
plt.show()

# -------------------------
# Plot correlation heatmap
# -------------------------
corr_matrix_new = X_new.corr()
plt.figure(figsize=(16, 12))
sns.heatmap(
    corr_matrix_new,
    annot=True,
    fmt=".2f",
    cmap="coolwarm",
    cbar=True,
    square=True,
)
plt.title("Feature Correlation Heatmap (After Handling Multicollinearity)", fontsize=18)
plt.show()
