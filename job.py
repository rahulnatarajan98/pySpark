import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructType, StringType, IntegerType, DecimalType
from pyspark.sql.functions import countDistinct, avg, stddev, format_number

class StartSpark():
    def __init__(self,appname,filepath,master='local[*]',config=None):
        self.appname = appname
        self.master = master
        self.config = config
        self.filepath = filepath
        self.session = None
        self.df = None
    
    def createSession(self):
        spark = SparkSession.builder.master(self.master).appName(self.appname)
        if self.config:
            spark = spark.config(self.config)

        self.session = spark.getOrCreate()

        return self.session

    def stopsession(self):
        self.session.stop()
        return None
    
    def readFile(self,final_struc):
        if self.session:
            ext = self.filepath.split('.')[-1]
            if ext == 'txt':
                self.df = self.session.read.text(self.filepath)
            elif ext == 'json':
                self.df = self.session.read.json(self.filepath)
            elif ext == 'csv':
                self.df = self.session.read.option("header",True).csv(self.filepath, schema=final_struc)
            else:
                print ("No extenstion found")
            
            return self.df
        
        else:
            print ("No active Spark Session")
    
    def selectQuery(self,query='*'):
        if self.df:
            return self.df.select(query).show()
        else:
            print ("No Data Frame Available")
    
    def addingColumn(self,newCol,oldCol):
        if self.df:
            return self.df.withColumn(newCol,df[oldCol]).show()
        else:
            print ("No Data Frame Available")



def main():
    try:
        obj = StartSpark(appname='Test',filepath='Car_Purchasing_Data.csv')
        spark = obj.createSession()

        dataSchema = [StructField('Customer Name',StringType(),True), 
            StructField('Gender',IntegerType(),True), 
            StructField('Age', DecimalType(), True),
            StructField('Annual Salary', DecimalType(), True) ]
        
        final_struc = StructType(fields=dataSchema)

        df = obj.readFile(final_struc)

        

        df.show()
        df.printSchema()
        
        
        df.select('*').show()
        df.select(['age','gender']).show()
        print (obj.selectQuery(query=['age','gender']))

        df.withColumn('newAge', df['Age']+2).show()

        #Creating Temproray View
        df.createOrReplaceTempView('mydata')
        results = spark.sql("SELECT * FROM mydata")
        results.show()

        #FILTER Operations
        print ("Filter Operations")
        df.filter((df['Age'] > 30.0) & (df['Gender'] == 1) ).select('*').show()

        df.filter(df['Customer Name'].rlike('^N')).show()

        #Aggregate fn
        df.groupBy("Gender").count().show()
        df.agg({'Annual Salary': 'sum'}).show()

        df.orderBy('Customer Name').show()
        df.orderBy(df['Customer Name'].desc()).show()
    
        #SQL Functions
        df.select(avg('Age').alias('avg_age')).show()

        std = df.select(stddev('Age').alias('std_dev'))
        std.select(format_number('std_dev',2).alias('StandardDeviation')).show()


    except Exception as e:
        print (e)

    finally:
        spark.stop()


if __name__=='__main__':
    main()
