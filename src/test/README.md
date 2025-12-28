# 🧪 Test Directory

This directory contains comprehensive test modules for the Real-Time Economic Analytics Platform.

## 📁 Test Files

### 1. `test_kafka_db_to_spark.py`
**Purpose**: Tests the complete data pipeline from SQLite → Kafka → Spark → MongoDB

**Features Tested**:
- Database initialization with test data
- Kafka topics initialization
- Data transfer from SQLite to Kafka
- Historical data re-ingestion
- Complete pipeline integration
- Data consistency verification

**Test Data**: Creates a temporary test database with sample products, clients, sales, and inventory

### 2. `test_mongodb_collections.py`
**Purpose**: Tests MongoDB collection display functionality for the admin interface

**Features Tested**:
- MongoDB connection and accessibility
- Data retrieval from MongoDB collections
- Collection statistics calculation
- Data export formats (CSV, JSON)
- Visualization data preparation
- Automatic test data creation

**Test Data**: Creates test collections if none exist for comprehensive testing

## 🚀 Running Tests

### Manual Execution (Recommended)

#### Run Kafka to Spark Pipeline Tests:
```bash
cd /home/yahya-bouchak/dashbord_simple
source venv/bin/activate
python -m src.test.test_kafka_db_to_spark manual
```

#### Run MongoDB Collections Tests:
```bash
cd /home/yahya-bouchak/dashbord_simple
source venv/bin/activate
python -m src.test.test_mongodb_collections manual
```

### Unit Test Execution

#### Run Kafka to Spark Pipeline Tests:
```bash
cd /home/yahya-bouchak/dashbord_simple
source venv/bin/activate
python -m unittest src.test.test_kafka_db_to_spark
```

#### Run MongoDB Collections Tests:
```bash
cd /home/yahya-bouchak/dashbord_simple
source venv/bin/activate
python -m unittest src.test.test_mongodb_collections
```

### Run All Tests
```bash
cd /home/yahya-bouchak/dashbord_simple
source venv/bin/activate
python -m unittest discover src/test -v
```

## 🎯 Test Coverage

### Data Pipeline Tests
✅ **Database Operations**: SQLite initialization, CRUD operations
✅ **Kafka Integration**: Topic creation, data production
✅ **Data Transfer**: SQLite to Kafka data mapping
✅ **Historical Processing**: Re-ingestion functionality
✅ **Pipeline Integration**: End-to-end data flow

### Admin Interface Tests
✅ **MongoDB Connection**: Client creation, database access
✅ **Data Retrieval**: Collection reading, DataFrame conversion
✅ **Statistics**: Numeric and categorical analysis
✅ **Export Functionality**: CSV and JSON formats
✅ **Visualization Data**: Chart data preparation

## 📊 Test Features

- **Automatic Test Data**: Creates realistic test data if none exists
- **Comprehensive Logging**: Detailed test execution logging
- **Error Handling**: Graceful handling of missing data or connections
- **Manual & Unit Test Modes**: Flexible execution options
- **Clean Environment**: Proper setup and teardown

## 🔧 Requirements

- Python 3.8+
- All dependencies from `requirements.txt`
- Running MongoDB instance
- Running Kafka instance
- Proper environment configuration

## 📝 Notes

- Tests use a temporary database (`test_analytics.db`) to avoid affecting production data
- Manual mode provides detailed output and progress tracking
- Unit test mode integrates with standard Python unittest framework
- Tests are designed to be non-destructive and safe to run in any environment