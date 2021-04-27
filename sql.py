import findspark
findspark.init()

from pyspark.sql import SparkSession

class Spark():
    def __init__(self,appname):
        self.appname = appname
        self.database = 'dvdrental'
        self.username = 'postgres'
        self.password = 'password'
        self.url = f'jdbc:postgresql://localhost:5432/{self.database}'
        self.session = None
        self.df = None
        self.createSession()
    
    def createSession(self):
        spark = SparkSession.builder.appName(self.appname).config("spark.jars", "dependencies/postgresql-42.2.14.jar")
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
            self.df = self.session.read.format("jdbc").options(
                url=self.url,
                dbtable = table,
                user=self.username,
                password=self.password,
                driver="org.postgresql.Driver"
            ).load()
            print(f"{table} - Table Loaded")
            return self.df
        else:
            print("No active session to load db")


def main():
    try:
        spark = Spark(appname='Postgresql Connect')

        actor = spark.readdb('actor')
        actor.show()

        address = spark.readdb('address')
        address.show()

        film = spark.readdb('film')
        film.filter((film['length']>20) & (film['length']<100)).select(['title','film_id']).show()

    except Exception as e:
        print (e)
    
    finally:
        spark.stopSession()


if __name__=='__main__':
    main()