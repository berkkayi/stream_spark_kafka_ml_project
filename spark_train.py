import findspark
import pandas as pd
pd.set_option("display.max_columns",None)
findspark.init("/opt/manual/spark")

from pyspark.sql import SparkSession ,  functions as F
spark = SparkSession.builder \
.master("yarn") \
.appName("Spark_project") \
.enableHiveSupport() \
.getOrCreate()


tables = ["application_train","application_test",
          "bureau","bureau_balance","installments_payments",
          "credit_card_balance","pos_cash_balance","previous_application"]
table = []
drop_col = []
categoric_cols = []
categoric_table = []
numeric_cols = []
numeric_table = []
for i in tables:
    data = spark.sql(f"select * from homecredit.{i}")
    
    for column in data.columns:
        null_count = data.filter( (F.col(column).isNull()) | (F.col(column) == "")).count()
        n = data.count()
        if(  null_count > 0 ):
            
            if null_count/n > 0.4:
                table.append(i)
                drop_col.append(column)
            else:
                if (data.select(column).dtypes[0][1] == 'string'):
                    categoric_cols.append(column)
                    categoric_table.append(i)
                else: 
                    numeric_cols.append(column)
                    numeric_table.append(i)
        else:
            if (data.select(column).dtypes[0][1] == 'string'):
                categoric_cols.append(column)
                categoric_table.append(i)
            else: 
                numeric_cols.append(column)
                numeric_table.append(i)
                
numeric = pd.DataFrame({"numeric_cols":numeric_cols,
                         "numeric_table":numeric_table})
categoric = pd.DataFrame({"categoric_cols":categoric_cols,
                        "categoric_table":categoric_table})
data = pd.DataFrame({"cols":numeric_cols + categoric_cols,
                     "tables":numeric_table + categoric_table})
col = ",".join(list(set(data["cols"])))


col7 = ",".join(list(set(data[data["tables"] == "application_train"]["cols"])))
application_train = spark.sql(f"select {col7} from homecredit.application_train")

col8 = ",".join(list(set(data[data["tables"] == "application_test"]["cols"])))
application_test = spark.sql(f"select {col8} from homecredit.application_test")


train_categoric = list(categoric[categoric["categoric_table"] == "application_test"]["categoric_cols"])
train_numeric = list(numeric[numeric["numeric_table"] == "application_test"]["numeric_cols"])
                
                
train = application_train.drop("sk_id_curr")
test = application_test.drop("sk_id_curr")
train_numeric.remove("sk_id_curr")                
                
to_be_onehotencoded_cols = []

for col_name in train_categoric:
    count = train.select(col_name).distinct().count()
    if count > 2:
        to_be_onehotencoded_cols.append(col_name)    
                
from pyspark.ml.feature import StringIndexer
# Will hold stringIndexer objects and column names
my_dict = {}

# Will collect StringIndexer ojects
string_indexer_objs = []

# Will collect StringIndexer output colnames
string_indexer_output_names = []

# Will collect OneHotEncoder output colnames
ohe_col_input_names = []
ohe_col_output_names = []

for col_name in train_categoric:
    my_dict[col_name+"_indexobj"] = StringIndexer() \
    .setHandleInvalid('skip') \
    .setInputCol(col_name) \
    .setOutputCol(col_name+"_indexed")
    
    string_indexer_objs.append(my_dict.get(col_name+"_indexobj"))
    string_indexer_output_names.append(col_name+"_indexed")
    if col_name in to_be_onehotencoded_cols:
        ohe_col_input_names.append(col_name+"_indexed")
        ohe_col_output_names.append(col_name+"_ohe")
        
from pyspark.ml.feature import OneHotEncoder
encoder = OneHotEncoder() \
.setInputCols(ohe_col_input_names) \
.setOutputCols(ohe_col_output_names)

string_indexer_col_names_ohe_exluded = list(set(string_indexer_output_names).difference(set(ohe_col_input_names)))

from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler().setHandleInvalid("skip") \
.setInputCols(train_numeric+string_indexer_col_names_ohe_exluded+ohe_col_output_names) \
.setOutputCol('unscaled_features')

label_col = ["target"]

label_indexer = StringIndexer().setHandleInvalid("skip") \
.setInputCol(label_col[0]) \
.setOutputCol("label")

from pyspark.ml.feature import StandardScaler
scaler = StandardScaler().setInputCol("unscaled_features").setOutputCol("features")

from pyspark.ml.classification import RandomForestClassifier
rf = RandomForestClassifier(numTrees=400, maxDepth=10, seed=42)
estimator = rf.setFeaturesCol("features").setLabelCol("label")

from pyspark.ml import Pipeline
pipeline_obj = Pipeline().setStages(string_indexer_objs+[encoder, assembler, label_indexer, scaler, estimator])

pipeline_model = pipeline_obj.fit(train)

pipeline_model.stages[-1].write().overwrite().save("project/homecredit_model")
pipeline_model.stages[-5].write().overwrite().save("project/encoder")
pipeline_model.stages[-4].write().overwrite().save("project/vector_assembler")
pipeline_model.stages[-2].write().overwrite().save("project/scaler")

test.write \
.format("csv") \
.overwrite() \
.mode("overwrite") \
.save("hdfs:///test.csv")

print("train finished!")
spark.stop()
