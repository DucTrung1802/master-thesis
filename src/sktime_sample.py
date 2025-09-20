import numpy as np
import pandas as pd
from sktime.classification.interval_based import TimeSeriesForestClassifier
from sktime.transformations.panel.compose import ColumnConcatenator
from sklearn.pipeline import Pipeline


class CustomTimeSeriesForestClassifier(TimeSeriesForestClassifier):
    feature_types = ["mean", "std", "slope"]

    def _extract_feature_importance_of_feature_type_from_tree_feature_importance(
        self, tree_feature_importance: np.array, feature_type: str
    ) -> np.array:
        """
        Extracting the feature importance corresponding from a feature type (eg. "mean", "std", "slope") from tree
        feature importance
        ----------
        tree_feature_importance : array-like of shape (n_features_in,)
            The feature importance per feature in an estimator, n_intervals x number of feature types
        feature_type : str
            feature type belonging to self.feature_types
        Returns
        -------
        self : array-like of shape (n_intervals,)
            Feature importance corresponding from a feature type
        """
        feature_index = np.argwhere(
            [
                feature_type == feature_type_recorded
                for feature_type_recorded in self.feature_types
            ]
        )[0, 0]

        feature_type_feature_importance = tree_feature_importance[
            [
                interval_index + feature_index
                for interval_index in range(
                    0, len(tree_feature_importance), len(self.feature_types)
                )
            ]
        ]

        return feature_type_feature_importance

    @property
    def feature_importances_(self, **kwargs) -> pd.DataFrame:

        all_importances_per_feature = {
            "mean": np.zeros(self.series_length),
            "std": np.zeros(self.series_length),
            "slope": np.zeros(self.series_length),
        }

        for tree_index in range(self.n_estimators):
            tree = self.estimators_[tree_index]
            tree_importances = tree.feature_importances_
            tree_intervals = self.intervals_[tree_index]
            for feature_type in self.feature_types:
                feature_type_importances = self._extract_feature_importance_of_feature_type_from_tree_feature_importance(
                    tree_importances, feature_type
                )
                for interval_index in range(self.n_intervals):
                    interval = tree_intervals[interval_index]
                    all_importances_per_feature[feature_type][
                        interval[0] : interval[1]
                    ] += feature_type_importances[interval_index]

        temporal_feature_importance = (
            pd.DataFrame(all_importances_per_feature)
            / self.n_estimators
            / self.n_intervals
        )
        return temporal_feature_importance


config = {
    "model_params": {
        "n_estimators": 5,
        "min_interval": 3,
    }
}

time_series_dataframe = pd.DataFrame(
    {
        "feature_0": [0.0, 0.0, 0.0, 1.0, 2.0, 3.0],
        "feature_1": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
        "feature_2": [-4.0, -5.0, -6.0, -7.0, -8.0, -9.0],
    },
    index=[
        "2022-01-01 00:00:00",
        "2022-01-01 01:00:00",
        "2022-01-01 02:00:00",
        "2022-01-01 03:00:00",
        "2022-01-01 04:00:00",
        "2022-01-01 05:00:00",
    ],
)

windowed_chunk_time_series_dataframe = pd.DataFrame(
    {
        "dim_0": [pd.Series([0.0, 0.0, 0.0]), pd.Series([1.0, 2.0, 3.0])],
        "dim_1": [pd.Series([10.0, 20.0, 30.0]), pd.Series([40.0, 50.0, 60.0])],
        "dim_2": [pd.Series([-4.0, -5.0, -6.0]), pd.Series([-7.0, -8.0, -9.0])],
    },
    index=["frame_0", "frame_1"],
)
windowed_chunk_time_series_dataframe_label = pd.DataFrame(
    {"y": [0, 1]}, index=["frame_0", "frame_1"]
)


if __name__ == "__main__":
    steps = [
        ("concatenate", ColumnConcatenator()),
        ("classify", TimeSeriesForestClassifier(**config["model_params"])),
    ]
    clf = Pipeline(steps)

    # ----------------------------------------- #
    # Create ColumnConcatenator Entry Dataframe #
    # ----------------------------------------- #
    concatenator = ColumnConcatenator()

    output_dataframe = concatenator.fit_transform(windowed_chunk_time_series_dataframe)

    expected_output_dataframe = pd.DataFrame(
        {
            0: [
                pd.Series([0.0, 0.0, 0.0, 10.0, 20.0, 30.0, -4.0, -5.0, -6.0]),
                pd.Series([1.0, 2.0, 3.0, 40.0, 50.0, 60.0, -7.0, -8.0, -9.0]),
            ]
        },
        index=["frame_0", "frame_1"],
    )
    expected_output_dataframe.index.names = ["instances"]
    output_dataframe.index.name = "instances"
    pd.testing.assert_frame_equal(output_dataframe, expected_output_dataframe)

    # ----------------------------------------- #
    #      How is the feature space build       #
    # ----------------------------------------- #
    clf["classify"].fit(
        expected_output_dataframe,
        windowed_chunk_time_series_dataframe_label.values.reshape(-1),
    )
    print(clf["classify"].intervals_)
    print(clf["classify"].estimators_[0].n_features_in_)

    # ----------------------------------------- #
    #            Feature Importance             #
    # ----------------------------------------- #
    steps = [
        ("concatenate", ColumnConcatenator()),
        ("classify", CustomTimeSeriesForestClassifier()),
    ]
    clf = Pipeline(steps)
    clf.fit(
        windowed_chunk_time_series_dataframe,
        windowed_chunk_time_series_dataframe_label.values.reshape(-1),
    )

    temporal_feature_importance = clf["classify"].feature_importances_

    separators = range(
        0,
        clf["classify"].series_length,
        len(windowed_chunk_time_series_dataframe.iloc[0, 0]),
    )

    ax = temporal_feature_importance.plot(figsize=(20, 10))
    for separator in separators:
        ax.vlines(
            separator,
            temporal_feature_importance.min().min(),
            temporal_feature_importance.max().max(),
            color="r",
            alpha=0.1,
        )

    fig = ax.get_figure()

    fig.savefig("./feature_importance.png")

    def feature_importance_in_dim(
        time_series_forest_classifier: CustomTimeSeriesForestClassifier,
        nb_of_series: int,
    ) -> pd.DataFrame:

        temporal_feature_importance = time_series_forest_classifier.feature_importances_
        separators = range(0, time_series_forest_classifier.series_length, nb_of_series)
        feature_importance_per_series_dict = {
            col: [
                temporal_feature_importance.loc[
                    start : start + time_series_forest_classifier.series_length, col
                ].mean()
                for start in separators
            ]
            for col in temporal_feature_importance
        }
        feature_importance_per_series_df = pd.DataFrame(
            feature_importance_per_series_dict
        )

        return feature_importance_per_series_df

    feature_importance_df = feature_importance_in_dim(
        clf["classify"], len(windowed_chunk_time_series_dataframe.iloc[0, 0])
    )
    feature_importance_df.index = windowed_chunk_time_series_dataframe.columns
