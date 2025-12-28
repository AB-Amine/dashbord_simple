#!/usr/bin/env python3
"""
Standalone script to run historical data re-ingestion.

This script can be run independently to process existing SQLite data
and publish it to Kafka for Spark to analyze.

Usage:
    python run_historical_reingestion.py

This is useful for:
- Initial system setup
- Periodic historical data updates
- Testing the re-ingestion process
- Batch processing of legacy data
"""

import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from utils.config import Config
from kafka.kafka_producer import KafkaProducer
from data.historical_data_reingestion import HistoricalDataReingestion


def main():
    """Main function to run historical data re-ingestion."""
    print("🎯 Historical Data Re-Ingestion Tool")
    print("=" * 50)
    
    try:
        # Initialize components
        config = Config()
        kafka_producer = KafkaProducer(config)
        reingestion = HistoricalDataReingestion(config, kafka_producer)
        
        print(f"📊 Configuration:")
        print(f"   SQLite DB: {config.SQLITE_DB_PATH}")
        print(f"   Kafka: {config.KAFKA_BOOTSTRAP_SERVERS}")
        print(f"   MongoDB: {config.SPARK_MONGODB_URI}")
        print()
        
        # Run re-ingestion
        print("🚀 Starting historical data re-ingestion...")
        success = reingestion.reingest_all_historical_data()
        
        if success:
            print()
            print("✅ Historical data re-ingestion completed successfully!")
            print()
            print("📊 What happens next:")
            print("   1. Historical data has been published to Kafka topics")
            print("   2. Spark Structured Streaming will automatically consume the data")
            print("   3. Analytics will be computed and stored in MongoDB")
            print("   4. Admin Interface will display complete historical + real-time results")
            print()
            print("🔄 To see the results:")
            print("   - Ensure Spark streaming is running: python -m src.analytics.spark_streaming_main")
            print("   - Wait a few moments for Spark to process the data")
            print("   - Refresh the Admin Interface to see updated analytics")
            return 0
        else:
            print()
            print("❌ Historical data re-ingestion failed.")
            print("   Check the logs for detailed error information.")
            return 1
            
    except Exception as e:
        print()
        print(f"❌ Fatal error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())