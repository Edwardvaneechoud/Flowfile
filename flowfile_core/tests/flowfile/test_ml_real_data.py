"""Train Model -> Apply Model end to end on CSV data shaped like a real Kaggle problem.

Reads two Titanic-shaped files (the test file has no target column, several features
have missing values, flags are derived as Int8), trains through the real worker and
scores the unlabeled rows, the way a user script or notebook does.
"""

from pathlib import Path

import polars as pl
import pytest

import flowfile_frame as ff

FEATURES = ["Pclass", "IsFemale", "Age", "Fare", "FamilySize", "HasCabin"]
N_TRAIN = 150
N_TEST = 50


def _passengers(start: int, n: int) -> pl.DataFrame:
    rows = []
    for i in range(start, start + n):
        female = (i * 7) % 5 in (0, 2)
        pclass = 1 + i % 3
        title = ("Mrs" if i % 2 else "Miss") if female else ("Mr" if i % 4 else "Master")
        rows.append(
            {
                "PassengerId": i + 1,
                "Survived": int(female),
                "Pclass": pclass,
                "Name": f"Surname{i}, {title}. Given{i}",
                "Sex": "female" if female else "male",
                "Age": None if i % 6 == 0 else float(2 + (i * 13) % 70),
                "SibSp": i % 3,
                "Parch": (i // 3) % 2,
                "Fare": round(8.05 + (4 - pclass) * 25 + (i * 3) % 17, 2),
                "Cabin": f"C{i}" if pclass == 1 and i % 4 == 1 else None,
            }
        )
    return pl.DataFrame(rows)


@pytest.fixture
def titanic_csvs(tmp_path: Path) -> tuple[Path, Path, pl.DataFrame]:
    train = _passengers(0, N_TRAIN)
    test = _passengers(N_TRAIN, N_TEST).with_columns(
        pl.when(pl.col("PassengerId") == N_TRAIN + 3).then(None).otherwise(pl.col("Fare")).alias("Fare")
    )
    train_path, test_path = tmp_path / "train.csv", tmp_path / "test.csv"
    train.write_csv(train_path)
    test.drop("Survived").write_csv(test_path)
    return train_path, test_path, test


def _score(train_path: Path, test_path: Path, model_type: str, params: dict | None = None):
    train = ff.read_csv(str(train_path))
    test = ff.read_csv(str(test_path))
    full = ff.concat([train, test], how="diagonal_relaxed").with_columns(
        ff.col("Name").str.extract(r",\s*([^.]+)\.", 1).alias("Title"),
        (ff.col("SibSp") + ff.col("Parch") + 1).alias("FamilySize"),
        ff.col("Cabin").is_not_null().cast(ff.Int8).alias("HasCabin"),
        (ff.col("Sex") == "female").cast(ff.Int8).alias("IsFemale"),
    )
    labeled = full.filter(ff.col("Survived").is_not_null())
    unlabeled = full.filter(ff.col("Survived").is_null())
    model = labeled.train_model(target="Survived", features=FEATURES, model_type=model_type, params=params)
    return unlabeled.wait_for(model).apply_model(upstream=model, output_column="prediction")


def _expected(test: pl.DataFrame) -> pl.DataFrame:
    return test.select(
        "PassengerId",
        pl.col("Survived").alias("expected"),
        pl.any_horizontal(pl.col("Age", "Fare").is_null()).alias("incomplete"),
    )


@pytest.mark.parametrize("model_type", ["logistic_regression", "linear_regression"])
def test_parametric_models_learn_the_signal_and_leave_incomplete_rows_unscored(titanic_csvs, model_type):
    train_path, test_path, test = titanic_csvs

    scored = _score(train_path, test_path, model_type).select("PassengerId", "prediction").collect()

    assert scored.height == N_TEST
    joined = scored.join(_expected(test), on="PassengerId", how="inner")
    assert joined.height == N_TEST
    assert joined.filter(pl.col("incomplete"))["prediction"].is_null().all()
    complete = joined.filter(~pl.col("incomplete"))
    assert complete.height > 0 and complete["prediction"].null_count() == 0
    predicted_class = (complete["prediction"] > 0.5).cast(pl.Int64)
    assert (predicted_class == complete["expected"]).all()


def test_knn_scores_every_row_with_a_class(titanic_csvs):
    train_path, test_path, _ = titanic_csvs

    scored = _score(train_path, test_path, "knn_classifier", {"k": 5}).collect()

    assert scored.height == N_TEST
    assert scored["prediction"].null_count() == 0
    assert set(scored["prediction"].to_list()) <= {0, 1}


def test_full_run_writes_the_submission(titanic_csvs, tmp_path):
    train_path, test_path, test = titanic_csvs
    out = tmp_path / "submission.csv"

    scored = _score(train_path, test_path, "logistic_regression")
    submission = scored.select("PassengerId", ff.col("prediction").cast(ff.Int64).alias("Survived"))
    submission.write_csv(str(out))
    run_info = submission.flow_graph.run_graph()

    assert run_info.success, [r for r in run_info.node_step_result if not r.success]
    written = pl.read_csv(out)
    assert written.columns == ["PassengerId", "Survived"]
    assert sorted(written["PassengerId"].to_list()) == sorted(test["PassengerId"].to_list())
