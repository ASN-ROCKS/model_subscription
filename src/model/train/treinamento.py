# Databricks notebook source
# MAGIC %pip install feature-engine scikit-plot
# MAGIC
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

import mlflow

from sklearn import tree
from sklearn import ensemble
from sklearn import model_selection
from sklearn import metrics
from sklearn import pipeline

from feature_engine import imputation

# COMMAND ----------

# DBTITLE 1,Construção da ABT

feature_lookups = [
    FeatureLookup(
      table_name='gold.gamersclub.gameplay',
      lookup_key="idJogador",
      timestamp_lookup_key="dtRef"
    ),
    FeatureLookup(
      table_name='gold.gamersclub.sales',
      lookup_key="idJogador",
      timestamp_lookup_key="dtRef"
    ),
]

fe = FeatureEngineeringClient()

training_set = fe.create_training_set(
  df= spark.table("gold.gamersclub.target_sub")  ,
  feature_lookups=feature_lookups,
  label='flSub',
  exclude_columns=['idJogador']
)

training_df = training_set.load_df().toPandas()

# COMMAND ----------

max_date = training_df['dtRef'].max()

df_oot = training_df[training_df['dtRef'] == max_date] # out of time
df_train = training_df[training_df['dtRef'] < max_date] # treinamento

# COMMAND ----------

columns = df_train.columns.tolist()
target = 'flSub'
to_exclude = ['dtRef', target]

features = list(set(columns) - set(to_exclude))
features.sort()

num_features = df_train[features].dtypes[df_train[features].dtypes != 'object'].index.tolist()

# COMMAND ----------

X_train, X_test, y_train, y_test = model_selection.train_test_split(
    df_train[features],
    df_train[target],
    test_size=0.2,
    random_state=42
)

# COMMAND ----------

# DBTITLE 1,EDA
X_train.isna().sum().sort_values()

# COMMAND ----------

mlflow.set_experiment("/Users/teomewhy@gmail.com/databricks_automl/model_subscription_gamersclub")

# COMMAND ----------

# DBTITLE 1,Imputação
to_imput_0 = [
    "evolucao_level_pct",
    "qtdAssinaturas",
    "qtdAssinaturasMes",
    "qtdChurn",
    "qtdChurnMes",
    "qtdMedalhas",
    "qtdMedalhasMes",
    "daysSinceFirstSub",
    "qtAcertosEstomagoo_pct",
    "qtAcertosBracoDireito_pct",
    "qtAcertosBracoEsquerdo_pct",
    "qtAcertosCabeca_pct",
    "qtAcertosPeito_pct",
    "qtAcertosPernaDireita_pct",
    "qtAcertosPernaEsquerda_pct",
]

with mlflow.start_run():

    mlflow.sklearn.autolog()

    imput_0 = imputation.ArbitraryNumberImputer(arbitrary_number=0, variables=to_imput_0)
    
    # clf = tree.DecisionTreeClassifier(min_samples_leaf=50, random_state=42)
    clf = ensemble.RandomForestClassifier(n_estimators=300, min_samples_leaf=50, n_jobs=-1, random_state=42)
    
    model_pipeline = pipeline.Pipeline([
        ('imputer', imput_0),
        ('model', clf),
    ])

    model_pipeline.fit(X_train, y_train)

    metrics_mlflow = {}

    y_proba = model_pipeline.predict_proba(X_train)
    y_pred = model_pipeline.predict(X_train)
    metrics_mlflow["acc_train"] = metrics.accuracy_score(y_train, y_pred)
    metrics_mlflow["auc_train"] = metrics.roc_auc_score(y_train, y_proba[:,1])

    y_proba_test = model_pipeline.predict_proba(X_test)
    y_pred_test = model_pipeline.predict(X_test)
    metrics_mlflow["acc_test"] = metrics.accuracy_score(y_test, y_pred_test)
    metrics_mlflow["auc_test"] = metrics.roc_auc_score(y_test, y_proba_test[:,1])

    y_proba = model_pipeline.predict_proba(df_oot[X_test.columns.tolist()])
    y_pred = model_pipeline.predict(df_oot[X_test.columns.tolist()])
    metrics_mlflow["acc_oot"] = metrics.accuracy_score(df_oot[target], y_pred)
    metrics_mlflow["auc_oot"] = metrics.roc_auc_score(df_oot[target], y_proba[:,1])

    mlflow.log_metrics(metrics_mlflow)

# COMMAND ----------

import scikitplot as skplt
import matplotlib.pyplot as plt

skplt.metrics.plot_lift_curve(y_test, y_proba_test)
plt.show()

# COMMAND ----------

# y_train.mean()
y_proba_test[:, 1].mean()

# COMMAND ----------

skplt.metrics.plot_ks_statistic(y_test, y_proba_test)
plt.show()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

import numpy as np

matriz_conf
# [ VN , FP 
#   FN   VP]

# [ 0  , -5
#  -10 , 15]

matriz_custo = np.array([[0,-5],[-10,25]])
matriz_conf = metrics.confusion_matrix( y_test, (y_proba_test[:,1] > 0.119).astype(int) )
np.multiply(matriz_conf, matriz_custo).sum()

# COMMAND ----------


