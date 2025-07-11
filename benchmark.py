import time
import csv
from multiprocessing import Pool

def simple_sentiment_analysis(text):
    if not text:
        return 'neutral'
    
    positive_words = ['good', 'great', 'excellent', 'awesome', 'love', 'happy']
    negative_words = ['bad', 'terrible', 'awful', 'hate', 'sad', 'horrible']
    
    text_lower = text.lower()
    pos_count = sum(1 for word in positive_words if word in text_lower)
    neg_count = sum(1 for word in negative_words if word in text_lower)
    
    if pos_count > neg_count:
        return 'positive'
    elif neg_count > pos_count:
        return 'negative'
    else:
        return 'neutral'

def sequential_processing(data_subset):
    """Sequential processing for comparison"""
    start_time = time.time()
    results = {'positive': 0, 'negative': 0, 'neutral': 0}
    
    for row in data_subset:
        if len(row) >= 6:
            text = row[5]  # Text is in column 5
            sentiment = simple_sentiment_analysis(text)
            results[sentiment] += 1
    
    end_time = time.time()
    return results, end_time - start_time

def multiprocessing_analysis(data_chunk):
    """Multiprocessing worker function"""
    results = {'positive': 0, 'negative': 0, 'neutral': 0}
    
    for row in data_chunk:
        if len(row) >= 6:
            text = row[5]  # Text is in column 5
            sentiment = simple_sentiment_analysis(text)
            results[sentiment] += 1
    
    return results

def parallel_processing(data, num_processes=2):
    """Parallel processing using multiprocessing"""
    start_time = time.time()
    
    chunk_size = len(data) // num_processes
    chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
    
    with Pool(num_processes) as pool:
        results = pool.map(multiprocessing_analysis, chunks)
    
    # Aggregate results
    final_result = {'positive': 0, 'negative': 0, 'neutral': 0}
    for result in results:
        for sentiment, count in result.items():
            final_result[sentiment] += count
    
    end_time = time.time()
    return final_result, end_time - start_time

if __name__ == "__main__":
    print("Loading data for performance benchmark...")
    
    try:
        # Load data
        with open('sampled_100k_sentiment140.csv', 'r', encoding='utf-8', errors='ignore') as file:
            reader = csv.reader(file)
            header = next(reader)
            data = list(reader)[:5000]  # Use smaller subset for testing
        
        print(f"Benchmarking with {len(data)} records...")
        
        # Sequential processing
        print("Running sequential processing...")
        seq_result, seq_time = sequential_processing(data)
        print(f"Sequential - Results: {seq_result}, Time: {seq_time:.2f}s")
        
        # Parallel processing with 2 cores
        print("Running parallel processing with 2 cores...")
        par_result_2, par_time_2 = parallel_processing(data, 2)
        print(f"Parallel (2 cores) - Results: {par_result_2}, Time: {par_time_2:.2f}s")
        print(f"Speedup (2 cores): {seq_time/par_time_2:.2f}x")
        
        # Save performance results
        with open('performance_results.txt', 'w') as f:
            f.write(f"Performance Benchmark Results\n")
            f.write(f"Dataset size: {len(data)} records\n")
            f.write(f"Sequential time: {seq_time:.2f}s\n")
            f.write(f"Parallel (2 cores) time: {par_time_2:.2f}s\n")
            f.write(f"Speedup: {seq_time/par_time_2:.2f}x\n")
            f.write(f"Sequential results: {seq_result}\n")
            f.write(f"Parallel results: {par_result_2}\n")
        
        print("Performance benchmark completed! Results saved to performance_results.txt")
        
    except Exception as e:
        print(f"Error in performance benchmark: {e}")