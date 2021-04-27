import findspark
findspark.init()

from pyspark.sql import SparkSession

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
    
    def readFile(self):
        if self.session:
            ext = self.filepath.split('.')[-1]
            if ext == 'txt':
                self.df = self.session.read.text(self.filepath)
            elif ext == 'json':
                self.df = self.session.read.json(self.filepath)
            elif ext == 'csv':
                self.df = self.session.read.csv(self.filepath)
            else:
                print ("No extenstion found")
            return self.df
        
        else:
            print ("No active Spark Session")

def main():
    obj = StartSpark(appname='Test',filepath='Car_Purchasing_Data.csv')
    spark = obj.createSession()
    df = obj.readFile()
    print (df)
    df.show()
    df.printSchema()
    df.columns
    df.describe()
    df.describe().show()
    df.select('*').show()
    spark.stop()


if __name__=='__main__':
    main()
