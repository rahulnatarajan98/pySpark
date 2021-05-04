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
        self.createSession()
    
    def createSession(self):
        spark = SparkSession.builder.master(self.master).appName(self.appname)
        if self.config:
            spark = spark.config(self.config)

        self.session = spark.getOrCreate()

        return self.session
    
    def readFile(self,schema=None, header=False,mode="FAILFAST", inferSchema=False):
        if self.session:
            ext = self.filepath.split('.')[-1] #data/Car_Purchasing_Data.csv - ['data/Car_Purchasing_Data','csv'] 
            if ext == 'txt':
                self.df = self.session.read.text(self.filepath)
            elif ext == 'json':
                self.df = self.session.read.json(self.filepath)
            elif ext == 'csv':
                self.df = self.session.read.option("header",header).option("mode", mode) # PERMISSIVE, DROPMALFORMED, FAILFAST
                if schema:
                    self.df = self.df.csv(self.filepath, schema=schema)
                else:
                    self.df = self.df.option("inferSchema", inferSchema).csv(self.filepath)
            else:
                print ("No extenstion found")
            
            return self.df
        
        else:
            print ("No active Spark Session")
    
    def selectQuery(self,query='*', count=20):
        if self.df:
            return self.df.select(query).show(count)
        else:
            print ("No Data Frame Available")
    
    def addingColumn(self,newCol,oldCol):
        if self.df:
            return self.df.withColumn(newCol,df[oldCol]).show()
        else:
            print ("No Data Frame Available")



def main():
    try:
        obj = StartSpark(appname='Test',filepath='data/Car_Purchasing_Data.csv')

        # Structure Schema - https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#data-types
        dataSchema = [StructField('Customer Name',StringType(),True), 
            StructField('Gender',IntegerType(),True), 
            StructField('Age', DecimalType(), True),
            StructField('Annual Salary', DecimalType(), True) ]
        
        final_struc = StructType(fields=dataSchema)

        #Reading File
        df = obj.readFile(schema=final_struc, header=True, mode="DROPMALFORMED")

        print("Data Frame Schema")
        df.printSchema()
        
        #Select
        print("\n\n#Select Functions")
        df.select('*').show()
        df.select(['age','gender']).show(5)
        obj.selectQuery(query=['age','gender'])

        print("\n\nCollect Method")
        coll = df.collect()[0:3]
        print (coll)
        print('\n')
        print(coll[0].Age)

        print("\n\nNew Column")
        df.withColumn('newAge', df['Age']+2).show()
        #withColumnRenamed

        #Creating Temproray View
        print("\n\nTemporary View")
        df.createOrReplaceTempView('mydata')

        print("Using SQL Statements")
        results = obj.session.sql("SELECT * FROM mydata")
        results.show()

        #FILTER Operations
        print ("\n\nFilter Operations") # &, |, ~ should be used
        df.filter((df['Age'] > 30.0) & (df['Gender'] == 1) ).select('*').show()

        print("\n\nRegex filter")
        df.filter(df['Customer Name'].rlike('^N')).show()

        #Groupby and Orderby
        print("\n\nGroupby")
        df.groupBy("Gender").count().show()

        print("\n\nOrderby - Ascending")
        df.orderBy('Customer Name').show()

        print("\n\nOrderby - Descending")
        df.orderBy(df['Customer Name'].desc()).show()
    
        #SQL Functions
        print("\n\nAggregators")
        df.select(avg('Age').alias('avg_age')).show()
        df.agg({'Annual Salary': 'sum'}).show()
        std = df.select(stddev('Age').alias('std_dev'))
        std.show()

        print("\n\n Rounding off")
        std.select(format_number('std_dev',2).alias('StandardDeviation')).show()


    except Exception as e:
        print (e)

    finally:
        obj.session.stop()


if __name__=='__main__':
    main()
