import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator



def main():
    spark = SparkSession.builder.appName('Logreg').getOrCreate()
    df = spark.read.csv('data/customer_churn.csv', header=True, inferSchema=True)

    df.show()


    assembler = VectorAssembler(inputCols=['Age','Total_Purchase', 'Account_Manager', 'Years', 'Num_Sites'], 
                            outputCol='features')

    output = assembler.transform(df)

    final_data = output.select(['features', 'churn'])

    train_data, test_data = final_data.randomSplit([0.7,0.3])

    lr_churn = LogisticRegression(labelCol='churn')

    trained_model = lr_churn.fit(train_data)

    summ = trained_model.summary

    summ.predictions.describe().show()

    pred_and_labels = trained_model.evaluate(test_data)

    pred_and_labels.predictions.show()



if __name__=='__main__':
    main()