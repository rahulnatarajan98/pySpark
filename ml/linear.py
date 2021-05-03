import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression


def main():
    spark = SparkSession.builder.appName('Cruise').getOrCreate()
    df = spark.read.csv('data/cruise_ship_info.csv', header=True, inferSchema=True)

    df.show()

    indexer = StringIndexer(inputCol='Cruise_line', outputCol='cruise_cat')
    indexed = indexer.fit(df).transform(df)

    assembler = VectorAssembler(inputCols=['Age','Tonnage', 'passengers', 'length', 'cabins', 'passenger_density', 'crew', 'cruise_cat'], 
                            outputCol='features')

    output = assembler.transform(indexed)

    final_data = output.select(['features', 'crew'])

    train_data, test_data = final_data.randomSplit([0.7,0.3])

    ship_lr = LinearRegression(labelCol='crew')

    trained_ship_model = ship_lr.fit(train_data)

    ship_results = trained_ship_model.evaluate(test_data)

    print(ship_results.rootMeanSquaredError)

    print(ship_results.r2)




if __name__=='__main__':
    main()