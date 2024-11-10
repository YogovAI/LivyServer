from flask import Flask, request, jsonify
import requests
import json
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

LIVY_URL = "http://192.168.1.14:8998"

@app.route('/submit_job', methods=['POST'])
def submit_job():
    # Connect to the remote Spark cluster using Spark Connect
    
    # spark = SparkSession.builder.remote("sc://192.168.1.14:15002").getOrCreate()
    
    import findspark
    from pyspark.sql import SparkSession
    
    findspark.init("/usr/local/spark/")

    #Connect to remote Spark cluster
    # spark = SparkSession.builder.config("spark.jars", "//usr//local//spark-3.5.1//assembly//target//scala-2.12//jars//*").remote("sc://192.168.1.14:15002").getOrCreate()
    spark = SparkSession.builder.config("/usr/local/spark-3.5.1/assembly/target/scala-2.12/jars/spark-connect_2.12-3.5.1.jar").remote("sc://192.168.1.14:15002").getOrCreate()
    
    # spark.addArtifacts("/usr/local/spark-3.5.1/assembly/target/scala-2.12/jars/org.apache.spark_spark-connect_2.12-3.5.1.jar")
    # spark.addArtifacts("/usr/local/spark-3.5.1/assembly/target/scala-2.12/jars/spark-connect_2.12-3.5.1.jar")

    # Create a DataFrame
    data = [("James", "Smith", "USA", "CA"),
        ("Michael", "Rose", "USA", "NY"),
        ("Robert", "Williams", "USA", "CA")]

    columns = ["First_Name", "Last_Name", "Country", "State"]
    df = spark.createDataFrame(data, columns)

    # Perform some transformations
    df_filtered = df.filter(df.State == "CA")

    # Show results
    # df_filtered.show()
    
    file_df = spark.read.csv("hdfs://master:9000/user/yogov/Fake.csv", header=True, inferSchema=True)
    file_df.createOrReplaceTempView("file_df_vw")
    # file_df_pd = file_df.toPandas()
    # print("count is : ", file_df_pd.shape()[0])
    # cnt = file_df.count()
    cnt = spark.sql("""select count(1) as cnt from file_df_vw""")
    cnt.show()
    
    # file_df_pd = file_df.toPandas()
    # print("count is : ", cnt)
    
    file_df.show()
    return jsonify({"status" : "Finished"})
    
    
if __name__ == '__main__':
    app.run(debug=True, port=5006)    