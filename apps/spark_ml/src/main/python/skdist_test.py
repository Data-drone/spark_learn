from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .appName("PySpark XGB Sk-Dist") \
        .getOrCreate()


from skdist.distribute.search import DistGridSearchCV
from sklearn.datasets import (
  load_iris
)
from xgboost import XGBClassifier

def main():
    cv = 5
    clf_scoring = "accuracy"
    reg_scoring = "neg_mean_squared_error"

    data = load_iris()
    X = data["data"]
    y = data["target"]

    grid = dict(
        learning_rate=[.05, .01],
        max_depth=[4, 6, 8],
        colsample_bytree=[.6, .8, 1.0],
        n_estimators=[100, 200, 300]
    )

    model = DistGridSearchCV(
        XGBClassifier(),
        grid, spark.sparkContext, cv=cv, scoring=clf_scoring
        )

    model.fit(X,y)
    # predictions on the driver
    preds = model.predict(X)
    probs = model.predict_proba(X)

    # results
    print("-- Grid Search --")
    print("Best Score: {0}".format(model.best_score_))
    print("Best colsample_bytree: {0}".format(model.best_estimator_.colsample_bytree))
    print("Best learning_rate: {0}".format(model.best_estimator_.learning_rate))
    print("Best max_depth: {0}".format(model.best_estimator_.max_depth))
    print("Best n_estimators: {0}".format(model.best_estimator_.n_estimators))


if __name__=='__main__':

    main()