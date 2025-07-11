import sys
import time
sys.path.append('/home/ec2-user/spark/python/')
sys.path.append('/home/ec2-user/spark/python/lib/py4j-0.10.9.7-src.zip')

from pyspark.sql import SparkSession

def test_cluster():
    try:
        spark = SparkSession.builder \
            .appName("T3MediumClusterTest") \
            .master("spark://10.0.0.4:7077") \
            .config("spark.executor.memory", "800m") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.cores", "1") \
            .config("spark.driver.maxResultSize", "1g") \
            .config("spark.executor.instances", "2") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        
        print("✓ Spark session created successfully!")
        print(f"Master: {spark.sparkContext.master}")
        print(f"Application ID: {spark.sparkContext.applicationId}")
        
        # Test with larger dataset now that we have more memory
        data = list(range(1, 10001))  # 10,000 numbers
        rdd = spark.sparkContext.parallelize(data, 4)  # 4 partitions
        
        print("Running distributed computation...")
        start_time = time.time()
        
        sum_result = rdd.sum()
        count_result = rdd.count()
        max_result = rdd.max()
        min_result = rdd.min()
        
        end_time = time.time()
        
        print(f"\nResults:")
        print(f"Sum: {sum_result}")
        print(f"Count: {count_result}")
        print(f"Max: {max_result}")
        print(f"Min: {min_result}")
        print(f"Processing time: {end_time - start_time:.2f} seconds")
        
        # Verify results
        expected_sum = sum(data)  # 50005000
        expected_count = len(data)  # 10000
        
        if sum_result == expected_sum and count_result == expected_count:
            print("✓ Cluster test PASSED!")
            result = True
        else:
            print("✗ Cluster test FAILED!")
            result = False
        
        spark.stop()
        return result
        
    except Exception as e:
        print(f"✗ Error: {e}")
        return False

if __name__ == "__main__":
    print("Testing Spark cluster with t3.small instances...")
    if test_cluster():
        print("✓ Cluster is ready for production applications!")
    else:
        print("✗ Cluster needs troubleshooting")