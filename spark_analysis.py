import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count

# =================================================================
# 1. WINDOWS & JAVA ENVIRONMENT FIXES
# =================================================================

# FIX A: For Java 18+ (Solves "getSubject is not supported")
os.environ["JDK_JAVA_OPTIONS"] = (
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.util=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
    "--add-opens=java.base/javax.security.auth=ALL-UNNAMED"
)

# FIX B: For Windows File Saving (Solves "winutils.exe" error)
# IMPORTANT: Point this to where you downloaded winutils.exe
os.environ["HADOOP_HOME"] = "C:\\hadoop" 
os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"

# Reset Spark submit args to ensure clean start
os.environ["PYSPARK_SUBMIT_ARGS"] = "pyspark-shell"

# =================================================================
# 2. SPARK SESSION INITIALIZATION
# =================================================================
spark = SparkSession.builder \
    .appName("Plant Health Analysis") \
    .config("spark.driver.host", "localhost") \
    .getOrCreate()

# Set log level to ERROR to hide the wall of "INFO" text
spark.sparkContext.setLogLevel("ERROR")

try:
    print("\n--- Starting Analysis ---\n")

    # 3. LOAD DATA
    # Uses header=True because your CSV has column names
    df = spark.read.csv("big_data.csv", header=True, inferSchema=True)

    print("Data Sample:")
    df.show(5)

    # 4. DATA AGGREGATION
    # Groups by 'label' and calculates averages for all sensors
    result = df.groupBy("label").agg(
        count("*").alias("record_count"),
        avg("Soil_Moisture").alias("avg_soil"),
        avg("Ambient_Temperature").alias("avg_temp"),
        avg("Humidity").alias("avg_humidity"),
        avg("Light_Intensity").alias("avg_light")
    )

    print("Summary Statistics by Label:")
    result.show()

    # 5. SAVE OUTPUT
    # .mode("overwrite") prevents errors if the folder already exists
    result.write.mode("overwrite").csv("spark_output", header=True)
    
    print("SUCCESS: Results saved to the 'spark_output' folder.")

except Exception as e:
    print(f"AN ERROR OCCURRED: {e}")

finally:
    # 6. SHUTDOWN
    spark.stop()
    print("\n--- Spark Session Closed ---")