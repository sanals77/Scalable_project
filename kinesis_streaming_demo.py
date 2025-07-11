import boto3
import json
import time
from collections import defaultdict

def simple_sentiment_analysis(text):
    if not text:
        return 'neutral'

    positive_words = ['good', 'great', 'excellent', 'awesome', 'love', 'happy', 'wonderful', 'amazing', 'fantastic', 'perfect']
    negative_words = ['bad', 'terrible', 'awful', 'hate', 'sad', 'horrible', 'worst', 'disgusting', 'annoying', 'stupid']

    text_lower = text.lower()
    pos_count = sum(1 for word in positive_words if word in text_lower)
    neg_count = sum(1 for word in negative_words if word in text_lower)

    if pos_count > neg_count:
        return 'positive'
    elif neg_count > pos_count:
        return 'negative'
    else:
        return 'neutral'

def process_kinesis_stream():
    """Fixed Kinesis stream processor with better error handling"""
    try:
        kinesis = boto3.client('kinesis', region_name='eu-north-1')

        # Get shard iterator
        response = kinesis.describe_stream(StreamName='twitter-stream')
        shard_id = response['StreamDescription']['Shards'][0]['ShardId']

        shard_iterator = kinesis.get_shard_iterator(
            StreamName='twitter-stream',
            ShardId=shard_id,
            ShardIteratorType='LATEST'
        )['ShardIterator']

        print("🔄 Enhanced Kinesis Stream Processing Started...")
        print("Processing incoming streaming data...")

        sentiment_counts = defaultdict(int)
        processed_count = 0
        start_time = time.time()

        while processed_count < 100:  # Process up to 100 records for demo
            # Get records from stream
            response = kinesis.get_records(ShardIterator=shard_iterator, Limit=10)
            records = response['Records']

            if records:
                for record in records:
                    try:
                        # Try different JSON parsing approaches
                        data_str = record['Data'].decode('utf-8').strip()

                        # Handle different JSON formats
                        if data_str.startswith('{') and data_str.endswith('}'):
                            data = json.loads(data_str)
                        else:
                            # Try parsing as newline-delimited JSON
                            lines = data_str.split('\n')
                            for line in lines:
                                if line.strip():
                                    try:
                                        data = json.loads(line.strip())
                                        break
                                    except:
                                        continue
                            else:
                                # If JSON parsing fails, treat as plain text
                                data = {'text': data_str}

                        text = data.get('text', data_str)
                        if text:
                            # Perform sentiment analysis
                            sentiment = simple_sentiment_analysis(text)
                            sentiment_counts[sentiment] += 1
                            processed_count += 1

                            # Print real-time updates every 5 records
                            if processed_count % 5 == 0:
                                elapsed = time.time() - start_time
                                print(f"⚡ Processed {processed_count} streaming records")
                                print(f"   Current sentiment counts: {dict(sentiment_counts)}")
                                print(f"   Processing rate: {processed_count/elapsed:.2f} records/sec")
                                print("-" * 50)

                    except Exception as e:
                        # Skip problematic records but continue processing
                        continue

            # Get next iterator
            shard_iterator = response.get('NextShardIterator')
            if not shard_iterator:
                break

            time.sleep(1)  # Poll every second

        # Final results
        elapsed = time.time() - start_time
        print(f"\n🎯 KINESIS STREAMING RESULTS:")
        print(f"Records processed: {processed_count}")
        print(f"Total time: {elapsed:.2f} seconds")
        print(f"Final sentiment counts: {dict(sentiment_counts)}")
        print(f"Average processing rate: {processed_count/elapsed:.2f} records/sec")

        # Save streaming results
        with open('kinesis_streaming_results.txt', 'w') as f:
            f.write("KINESIS REAL-TIME STREAMING RESULTS\n")
            f.write("=" * 40 + "\n\n")
            f.write(f"Records processed: {processed_count}\n")
            f.write(f"Processing time: {elapsed:.2f}s\n")
            f.write(f"Sentiment results: {dict(sentiment_counts)}\n")
            f.write(f"Processing rate: {processed_count/elapsed:.2f} records/sec\n")

        print("✓ Kinesis streaming results saved!")

    except Exception as e:
        print(f"Kinesis streaming error: {e}")
        print("📝 Note: Ensure Kinesis stream 'twitter-stream' exists and has data")

if __name__ == "__main__":
    print("🌊 ENHANCED KINESIS REAL-TIME STREAMING DEMO")
    process_kinesis_stream()