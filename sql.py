from pyspark.sql import SparkSession
from decouple import config

class Spark():
    def __init__(self,appname):
        self.appname = appname
        self.database = config('DB_NAME')
        self.username = config('DB_USERNAME')
        self.password = config('DB_PASSWORD')
        self.url = f"jdbc:postgresql://{config('DB_HOST')}/{self.database}"
        self.session = None
        self.createSession()
    
    def createSession(self):
        spark = SparkSession.builder.appName(self.appname).config("spark.jars", "dependencies/postgresql-42.2.20.jar") #https://jdbc.postgresql.org/download.html
        self.session = spark.getOrCreate()
        return self.session
    
    def stopSession(self):
        if self.session:
            self.session.stop()
            print ("Session Closed")
        else:
            print("No active session")
    
    def readdb(self, table):
        if self.session:
            df = self.session.read.format("jdbc").options(
                url=self.url,
                dbtable = table,
                user=self.username,
                password=self.password,
                driver="org.postgresql.Driver"
            ).load()
            print(f"{table} - Table Loaded")
            return df
        else:
            print("No active session to load db")
    
    def joindf(self, df1, col1, df2, col2, jtype):
        if self.session:
            df = df1.join(df2,df1[col1] == df2[col2], jtype).drop(df1[col1]) #inner, left, right, full
            return df
        else:
            print("No active session to join db")


def main():
    try:
        spark = Spark(appname='Postgresql Connect')

        payment = spark.readdb('payment')
        staff = spark.readdb('staff')
        customer = spark.readdb('customer')

        print('\n\njoin')
        joindf = payment.join(staff, payment.staff_id == staff.staff_id, "inner") #leftouter, rightouter, inner ....
        joindf.select(['payment_id', 'customer_id', 'rental_id', 'amount', 'payment_date', 'first_name', 'last_name']).show()

        joindf = joindf.withColumnRenamed('first_name', 'staff_first_name')\
            .withColumnRenamed('last_name', 'staff_last_name')\
            .withColumnRenamed('email', 'staff_email')\
        
        
        
        df = joindf.join(customer, joindf.customer_id == customer.customer_id, "inner").drop(customer.customer_id).drop(joindf.store_id)
        df.select(['first_name', 'last_name', 'payment_id', 'amount', 'payment_date', 'staff_first_name', 'staff_last_name', 'staff_email']).show()

        
        #Using Method
        print("\n\nUsing Method")
        objjoindf = spark.joindf(payment, 'staff_id', staff, 'staff_id', "inner")
        objjoindf.show()

        df = df.drop('active', 'address_id', 'last_update', 'staff_id', 'picture')

        print('\n\nWriting into file')
        
        df.write\
            .format("csv")\
            .mode("overwrite")\
            .option("path", "data/csv/")\
            .partitionBy("staff_first_name","store_id")\
            .save()
        #mode - append, overwrite, errorIfExists, ignore
        #.option("maxRecordsPerFile",1000)
        #.saveAsTable(<tableName>)
        

    except Exception as e:
        print (e)


if __name__=='__main__':
    main()