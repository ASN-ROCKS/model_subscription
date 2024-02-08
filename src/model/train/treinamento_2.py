# Databricks notebook source
# MAGIC %pip install feature-engine scikit-plot
# MAGIC
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from sklearn import tree
from sklearn import ensemble
from sklearn import model_selection
from sklearn import metrics
from sklearn import pipeline

from feature_engine import imputation

df = spark.table("gold.gamersclub.abt_subs").toPandas()
df.head()

# COMMAND ----------

target = "flSub"
to_remove = ["dtRef", target]

features = list(set(df.columns.tolist()) - set(to_remove))

num_features = df[features].dtypes[df[features].dtypes != 'object'].index.tolist()
cat_feature = list(set(features)-set(num_features))

# COMMAND ----------

X_train, X_test, y_train, y_test = model_selection.train_test_split(df[features],
                                                                    df[target],
                                                                    test_size=0.2,
                                                                    random_state=42)

# COMMAND ----------

X_train.isna().sum().sort_values(ascending=False)

# COMMAND ----------

to_imput_0 = [
    'qtdChurnMes',
    'daysSinceFirstSub',
    'qtdMedalhasMes',
    'qtdChurn',
    'qtdAssinaturasMes',
    'qtdMedalhas',
    'qtdAssinaturas',
    'evolucao_level_pct',
    'qtAcertosPernaEsquerda_pct',
    'qtAcertosPeito_pct',
    'qtAcertosBracoEsquerdo_pct',
    'qtAcertosBracoDireito_pct',
    'qtAcertosCabeca_pct',
    'qtAcertosEstomagoo_pct',
    'qtAcertosPernaDireita_pct',
]

imputer_0 = imputation.ArbitraryNumberImputer(arbitrary_number=0, variables=to_imput_0)

# COMMAND ----------

clf = ensemble.RandomForestClassifier(n_estimators=300, min_samples_leaf=50, n_jobs=-1)

model_pipeline = pipeline.Pipeline(
    [("imputer", imputer_0),
     ("model", clf)]
)

# COMMAND ----------

model_pipeline.fit(X_train, y_train)

# COMMAND ----------

y_train_proba = model_pipeline.predict_proba(X_train)
proba_train = y_train_proba[:, 1]

y_test_proba = model_pipeline.predict_proba(X_test)
proba_1 = y_test_proba[:, 1]
proba_1

metrics.roc_auc_score(y_test, proba_1)

# COMMAND ----------

import numpy as np
import pandas as pd
from scipy import optimize as opt

m_cost = np.array(
    [[0,-5],
     [-10,25]]
)


def calc_money(m_cost, m_conf):
    '''Calcula o valor de garana dado duas matrizes'''
    return np.multiply(m_cost, m_conf).sum()


def otimiza_corte(m_cost, proba, y):
    '''Otimiza dee forma exaustiva todas a probabilidades para o melhor ponto de corte'''
    
    data = {"proba":[], "cost":[]}
    
    for p in np.unique(proba):
        m_conf = metrics.confusion_matrix(y, proba > p)
        data["cost"].append(calc_money(m_cost, m_conf))
        data["proba"].append(p)

    data = pd.DataFrame(data)
    max_cost = data["cost"].max()
    return data[data["cost"]==max_cost].values[0].tolist()

otimiza_corte(m_cost, proba_train, y_train)

# COMMAND ----------

def otimiza_opt(p, m_cost, proba, y):
    '''Função para ser utilizar no otimizador'''
    m_conf = metrics.confusion_matrix(y, proba > p)
    return -1 * calc_money(m_cost, m_conf)

opt.minimize(
    fun=otimiza_opt,
    x0=0.07,
    args=(m_cost, proba_train, y_train),
    method='Nelder-Mead',
    options={"maxiter":1000}
)

# COMMAND ----------

def f(x):
    return x[0] ** 2 + x[1]

opt.minimize(fun=f, x0=[5, 5], method='Nelder-Mead', b)

# COMMAND ----------

8.785e+15
