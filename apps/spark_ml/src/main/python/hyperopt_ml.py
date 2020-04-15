from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .appName("PySpark XGB hyperopt") \
        .getOrCreate()

from hyperopt import hp
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.datasets import load_iris
from sklearn.metrics import precision_score
import xgboost as xgb
from hyperopt import fmin, tpe, STATUS_OK, STATUS_FAIL, SparkTrials


data = load_iris()
X = data["data"]
y = data["target"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=42)

# hyperopt xgb
xgb_clf_params = {
    'eta':              hp.choice('eta',              np.arange(0.1, 0.2, 0.3)),
    'max_depth':        hp.choice('max_depth',        np.arange(5, 16, 1, dtype=int)),
    'objective':        'multi:softprob',
    'num_class':        3,
    'n_estimators':     100,
}
xgb_fit_params = {
    'eval_metric': 'mlogloss',
    'early_stopping_rounds': 10,
    'verbose': False
}
xgb_para = dict()
xgb_para['clf_params'] = xgb_clf_params
xgb_para['fit_params'] = xgb_fit_params
xgb_para['loss_func' ] = lambda y, pred: precision_score(y, pred, average='weighted')


class HPOpt(object):

    def __init__(self, x_train, x_test, y_train, y_test):
        self.x_train = x_train
        self.x_test  = x_test
        self.y_train = y_train
        self.y_test  = y_test

    def process(self, fn_name, space, trials, algo, max_evals):
        fn = getattr(self, fn_name)
        try:
            result = fmin(fn=fn, space=space, algo=algo, max_evals=max_evals, trials=trials)
        except Exception as e:
            return {'status': STATUS_FAIL,
                    'exception': str(e)}
        return result, trials

    def xgb_clf(self, para):
        clf = xgb.XGBClassifier(**para['clf_params'])
        return self.train_clf(clf, para)
    

    def train_clf(self, clf, para):
        clf.fit(self.x_train, self.y_train,
                eval_set=[(self.x_train, self.y_train), (self.x_test, self.y_test)],
                **para['fit_params'])
        pred = clf.predict(self.x_test)
        loss = para['loss_func'](self.y_test, pred)
        return {'loss': loss, 'status': STATUS_OK}

obj = HPOpt(X_train, X_test, y_train, y_test)

xgb_opt = obj.process(fn_name='xgb_clf', space=xgb_para, trials=SparkTrials(parallelism=2), algo=tpe.suggest, max_evals=100)

print(xgb_opt)