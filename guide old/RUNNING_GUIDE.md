# Real-Time Economic Analytics Platform - Running Guide

## 🚀 Quick Start

This guide provides step-by-step instructions for running the Real-Time Economic Analytics Platform.

## 📋 Prerequisites

### System Requirements
- **Operating System**: Linux, macOS, or Windows (WSL recommended for Windows)
- **Python**: Python 3.8 or higher
- **Java**: Java 8 or higher (required for Spark)
- **Docker**: Docker 20.10 or higher (for Kafka and MongoDB)
- **Memory**: Minimum 8GB RAM recommended
- **Disk Space**: Minimum 2GB free disk space

### Required Services
- **Kafka** (for data streaming)
- **MongoDB** (for analytics results storage)
- **SQLite** (for initial data storage - included with Python)

## 🛠️ Installation

### 1. Clone the Repository

```bash
git clone https://github.com/your-repo/real-time-analytics-platform.git
cd real-time-analytics-platform
```

### 2. Set Up Python Environment

#### Option A: Using Virtual Environment (Recommended)

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate  # Linux/macOS
# OR
venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt
```

#### Option B: Using System Python

```bash
# Install dependencies globally
pip install -r requirements.txt
```

### 3. Start Required Services

```bash
# Start Kafka and MongoDB using Docker Compose
docker-compose up -d

# Wait for services to be ready (about 30 seconds)
sleep 30

# Verify services are running
docker-compose ps
```

Expected output:
```
      Name                     Command               State           Ports
--------------------------------------------------------------------------------
kafka          /etc/confluent/docker/run        Up      0.0.0.0:9092->9092/tcp
mongodb        docker-entrypoint.sh mongod      Up      0.0.0.0:27017->27017/tcp
zookeeper      /etc/confluent/docker/run        Up      0.0.0.0:2181->2181/tcp
```

## 🚀 Running the Application

### Option 1: Run Main Application (Recommended)

```bash
# Run the main application with Streamlit UI
python src/main.py
```

This will start the Streamlit web interface. After a few moments, you should see:
```
You can now view your Streamlit app in your browser.

  Local URL: http://localhost:8501
  Network URL: http://192.168.1.100:8501
```

Open your browser to `http://localhost:8501` to access the application.

### Option 2: Run Individual Components

#### Start Spark Analytics Processor

```bash
# Run Spark analytics in background
python src/analytics/spark_analytics.py &
```

#### Start Kafka Producer (for testing)

```bash
# Run Kafka producer for manual testing
python src/kafka/kafka_producer.py
```

## 📥 Data Processing Workflow

### 1. Prepare Your Data

Ensure you have an Excel file with the following sheets:
- **Products**: Product information (id, name, quantity, price, etc.)
- **Clients**: Client information (id, name, contact details)
- **Invoices**: Sales invoices (id, client_id, date, total)
- **Invoice_Details**: Invoice line items (id, facture_id, product_id, quantity, price)

### 2. Upload Data Through UI

1. Open the Streamlit interface at `http://localhost:8501`
2. Select **Accountant** from the sidebar menu
3. Click **Choose an Excel file** and select your data file
4. The system will automatically process the data through the pipeline

### 3. View Analytics Results

1. Switch to **Administrator** view in the sidebar
2. Navigate through the tabs to see:
   - **Product Winners**: Top 10 most profitable products
   - **Loss-Risk Products**: Products at risk of loss
   - **Client Promotions**: Client loyalty and promotion eligibility
3. Click **Refresh Data** to update the dashboard

## 🎛️ Command Line Interface

### Process Data File Directly

```bash
# Process a specific Excel file
python -c "
from main.application import RealTimeAnalyticsApplication
app = RealTimeAnalyticsApplication()
if app.initialize_system():
    app.process_data_file('path/to/your/data.xlsx')
    print('Data processing completed!')
else:
    print('Failed to initialize system')
"
```

### Start Spark Processing Only

```bash
# Start Spark processing in background
python -c "
from main.application import RealTimeAnalyticsApplication
app = RealTimeAnalyticsApplication()
if app.initialize_system():
    app.start_spark_processing()
    print('Spark processing started in background')
else:
    print('Failed to start Spark processing')
"
```

### Stop All Processing

```bash
# Gracefully shutdown all components
python -c "
from main.application import RealTimeAnalyticsApplication
app = RealTimeAnalyticsApplication()
app.shutdown()
print('Application shutdown completed')
"
```

## 🧪 Testing

### Run Code Structure Tests

```bash
# Test the refactored code structure
python test_code_structure.py
```

### Run Full Tests (if dependencies installed)

```bash
# Run comprehensive tests
python test_refactored_code.py
```

## 📊 Monitoring

### Check Kafka Topics

```bash
# List all Kafka topics
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

### Check MongoDB Collections

```bash
# Connect to MongoDB
docker-compose exec mongodb mongosh stock_management

# Show collections
show collections

# Query data
db.product_winners.find().pretty()
db.loss_risk_products.find().pretty()
db.clients.find().pretty()
```

### Check Spark UI

```bash
# Spark UI is available at
# http://localhost:4040 (when Spark jobs are running)
```

## 🛑 Shutdown

### Stop the Application

1. **Stop Streamlit**: Press `Ctrl+C` in the terminal where Streamlit is running
2. **Stop Services**:

```bash
# Stop Docker services
docker-compose down
```

### Clean Up

```bash
# Remove temporary files
rm -rf /tmp/hdfs_storage /tmp/checkpoints

# Remove Python cache
find . -type d -name "__pycache__" -exec rm -r {} +
find . -name "*.pyc" -delete
```

## 🔧 Troubleshooting

### Common Issues

#### Kafka Connection Errors

**Symptom**: `Failed to initialize Kafka topics`

**Solution**:
```bash
# Check if Kafka is running
docker-compose ps

# Restart Kafka
docker-compose restart kafka

# Wait 30 seconds and try again
sleep 30
```

#### MongoDB Connection Errors

**Symptom**: `Failed to connect to MongoDB`

**Solution**:
```bash
# Check if MongoDB is running
docker-compose ps

# Restart MongoDB
docker-compose restart mongodb

# Wait 10 seconds and try again
sleep 10
```

#### Spark Initialization Errors

**Symptom**: `Failed to initialize Spark session`

**Solution**:
```bash
# Check Java installation
java -version

# Ensure JAVA_HOME is set
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac))))
```

#### Memory Issues

**Symptom**: `Out of memory` errors

**Solution**:
```bash
# Increase Docker memory allocation
# In Docker Desktop: Settings > Resources > Memory

# Or limit Spark memory usage
export SPARK_DRIVER_MEMORY=2g
export SPARK_EXECUTOR_MEMORY=2g
```

## 📚 Environment Variables

You can customize the application using environment variables:

```bash
# Set environment variables
export DB_NAME="my_database"
export MONGO_URI="mongodb://localhost:27017/"
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export SPARK_APP_NAME="MyAnalyticsApp"

# Or use a .env file
cp .env.example .env
nano .env  # Edit the file
source .env
```

## 📖 Logs

### View Application Logs

```bash
# View Streamlit logs
# (in the terminal where you ran python src/main.py)

# View Docker service logs
docker-compose logs -f kafka
docker-compose logs -f mongodb
```

### Log File Locations

- **Application logs**: Console output (consider redirecting to file)
- **Spark logs**: `/tmp/spark-logs/` (if configured)
- **Docker logs**: Docker container logs

## 🎯 Best Practices

### Data Processing

1. **Start with small datasets** for testing
2. **Monitor resource usage** during processing
3. **Use the Refresh button** to update analytics views
4. **Check logs** for any warnings or errors

### Development

1. **Use virtual environments** to isolate dependencies
2. **Follow the OOP structure** when adding new features
3. **Add proper error handling** for new functionality
4. **Write tests** for new components

### Production

1. **Set up proper monitoring** for Kafka, MongoDB, and Spark
2. **Configure logging** to rotate log files
3. **Use environment variables** for configuration
4. **Set up health checks** for the Streamlit application

## 📞 Support

For issues and questions:

1. **Check the logs** first for error messages
2. **Review this guide** for common issues
3. **Consult the documentation**:
   - `OOP_README.md` - Class documentation
   - `REFACTORING_SUMMARY.md` - Technical details
4. **Open an issue** on GitHub with detailed error information

## 🎉 Success!

You should now have the Real-Time Economic Analytics Platform running with:
- ✅ Responsive Streamlit UI
- ✅ Real-time data processing with Spark
- ✅ Reliable Kafka messaging
- ✅ Analytics results in MongoDB
- ✅ Comprehensive monitoring and logging

The application is production-ready and can handle real-world data processing workloads!