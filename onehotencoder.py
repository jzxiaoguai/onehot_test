from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.mllib.util import MLUtils
from pyspark.ml.feature import VectorAssembler
from pyspark.mllib.regression import LabeledPoint
from pyspark.sql.functions import col
import pickle

sc = SparkContext(appName="IndexerStep")
sqlContext = SQLContext(sc)
#0.Read data
data = sqlContext.read.load("hdfs://")       #your file path,pay attention to the format you saved,you can add ".format()" to set
print data.select("label","tag").first()     #read data in a DataFrame

#1.oneHotEncoder
from pyspark.ml.feature import OneHotEncoder, StringIndexer
#feature--feature_Index--feature_Vec,feature_Vec is what we need
#1-1 Example
'''
print "try v1:"
stringIndexer = StringIndexer(inputCol="v1", outputCol="v1_Index")
model = stringIndexer.fit(data)
indexed = model.transform(data)
encoder = OneHotEncoder(dropLast=False, inputCol="v1_Index", outputCol="v1_Vec")  #dropLast=False if v1 have three categories and then generate three binary varibles
encoded = encoder.transform(indexed)
'''
#1-2 Deal with multiple attribute 
#Use a loop to process variables and handle part of variables
list_i=[1, 2, 3, 4, 5, 7, 8, 11, 24]          #varible list need to be binarization
print 'step 1'
for i in list_i:
        init_val='v'+str(i)
        mid_val=init_val+'_Index'
        out_val=init_val+'_Vec'
        stringIndexer = StringIndexer(inputCol=init_val, outputCol=mid_val)
        model = stringIndexer.fit(data)
        indexed = model.transform(data)
        encoder = OneHotEncoder(dropLast=False, inputCol=mid_val, outputCol=out_val)
        data = encoder.transform(indexed)      #loop
        print data.select(init_val,mid_val,out_val).first()

#2. Merge 
print 'step 2'
val_name=['v1_Vec', 'v2_Vec', 'v3_Vec', 'v4_Vec', 'v5_Vec', 'v6', 'v7_Vec', 'v8_Vec', 'v9', 'v10', 'v11_Vec', 'v12', 'v13', 'v14', 'v15', 'v16','v17', 'v18', 'v19', 'v20', 'v21', 'v22', 'v23', 'v24_Vec','v25', 'v26']
assembler = VectorAssembler(
    inputCols=val_name,
    outputCol="features")
output = assembler.transform(data)
print output.select("label","features").first()    #features is a sparse vector

#3.Save as libsvm format
#some tips about pyspark.ml.linalg.Vector and pyspark.mllib.linalg.Vector reference URL：https://stackoverflow.com/questions/41074182/cannot-convert-type-class-pyspark-ml-linalg-sparsevector-into-vector
#it may solve your TypeError: Cannot convert type <class 'pyspark.ml.linalg.SparseVector'> into Vector

print 'step 3'
from pyspark.mllib.linalg import Vectors as MLLibVectors
result =(output.select(col("label"),col("features")).rdd.map(lambda row: LabeledPoint(row.label,MLLibVectors.fromML(row.features))))
MLUtils.saveAsLibSVMFile(result, "hdfs://")
