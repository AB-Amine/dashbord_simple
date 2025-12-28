# Advanced Analytics System Usage Guide

## Overview
This guide provides step-by-step instructions for running the complete advanced analytics system with real-time processing every 10 seconds.

## System Components

### 1. MongoDB Database
- **Purpose**: Primary data storage for products, inventory, sales, and analytics results
- **Default URI**: `mongodb://localhost:27017/`
- **Database**: `stock_management`

### 2. Apache Kafka
- **Purpose**: Real-time data streaming between components
- **Required Topics**:
  - `topic-inventory-updates`
  - `topic-clients`
  - `topic-raw-sales`

### 3. Spark Streaming Application
- **Purpose**: Advanced analytics processing every 10 seconds
- **File**: `src/analytics/spark_streaming_main.py`

### 4. Real-time Kafka Producer
- **Purpose**: Continuous data import from MongoDB to Kafka
- **File**: `src/kafka/kafka_real_time_producer.py`

### 5. Admin Interface
- **Purpose**: Web-based dashboard for viewing analytics
- **File**: `src/main.py`

## Prerequisites

### Required Services
1. **MongoDB**: Must be running and accessible
2. **Kafka**: Must be running with required topics
3. **Python 3.8+**: With all dependencies installed

### Python Dependencies
```bash
pip install pyspark confluent-kafka pymongo pandas streamlit
```

## Starting the System

### Step 1: Start MongoDB
```bash
# Start MongoDB service
sudo systemctl start mongod

# Verify MongoDB is running
mongo --eval "db.runCommand({ping: 1})"
```

### Step 2: Start Kafka
```bash
# Start Zookeeper (required for Kafka)
zookeeper-server-start.sh /path/to/kafka/config/zookeeper.properties &

# Start Kafka server
kafka-server-start.sh /path/to/kafka/config/server.properties &

# Create required topics
kafka-topics.sh --create --topic topic-inventory-updates --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
kafka-topics.sh --create --topic topic-clients --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
kafka-topics.sh --create --topic topic-raw-sales --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# Verify topics exist
kafka-topics.sh --list --bootstrap-server localhost:9092
```

### Step 3: Start Real-time Kafka Producer
```bash
# Navigate to project directory
cd /home/yahya-bouchak/dashbord_simple

# Activate virtual environment (if using one)
source venv/bin/activate

# Start the real-time producer
python -m src.kafka.kafka_real_time_producer

# Expected output:
# 🚀 Starting continuous Kafka data import (every 10 seconds)
# 📡 Press Ctrl+C to stop...
# 🎯 Starting real-time data import to Kafka...
# ✅ Products: X records sent to Kafka
# ✅ Clients: Y records sent to Kafka
# ✅ Sales: Z records sent to Kafka
# ✅ Inventory: W records sent to Kafka
# 🎉 Real-time data import completed: 4/4 topics updated
```

### Step 4: Start Spark Streaming Application
```bash
# In a new terminal window
cd /home/yahya-bouchak/dashbord_simple
source venv/bin/activate

# Start Spark streaming
python -m src.analytics.spark_streaming_main

# Expected output:
# ✅ Spark session initialized: RealTimeAnalytics
# 🚀 Spark streaming started – waiting for Kafka events...
# 📡 Connecting to Kafka topics: sales='topic-raw-sales', inventory='topic-inventory-updates', clients='topic-clients'
# ✅ Successfully connected to all Kafka topics
# 📈 New sales data detected - recalculating product winners with economic analysis
# ✅ Enriched product winners with advanced analytics written to MongoDB
# 📊 Top product: Product Name with €X profit
# 🏆 Performance score: Y
# 📈 Sales velocity: Z units/day
# 🔄 Stock turnover: W
```

### Step 5: Start Admin Interface
```bash
# In a new terminal window
cd /home/yahya-bouchak/dashbord_simple
source venv/bin/activate

# Start Streamlit interface
python -m src.main

# Expected output:
# 🎯 Starting NEW User Interface
# 📱 Page configuration set
# 🚀 Real-Time Analytics Platform ready
# 
# Streamlit will open in your default browser at http://localhost:8501
```

## Using the Admin Interface

### Access the Interface
1. Open your web browser
2. Navigate to: `http://localhost:8501`
3. You should see the "Real-Time Economic Analytics Platform"

### Navigate to Advanced Analytics
1. **Main Menu**: Click the sidebar menu icon (☰)
2. **Administrator Section**: Select "👨‍🔬 Administrator"
3. **Analytics Dashboard**: Click on the "📈 Analytics" tab

### View Advanced Metrics

#### Product Winners Section
- **Summary Table**: Shows top products with all metrics
- **Profit Distribution Chart**: Visualizes profit by product
- **Revenue vs Cost Analysis**: Shows financial performance
- **Detailed Economic Metrics**: All financial calculations
- **Advanced Performance Metrics**: New metrics including:
  - **Sales Velocity**: Units sold per day
  - **Stock Turnover**: Inventory efficiency
  - **Performance Score**: Comprehensive ranking
  - **Days in Stock**: Time since last sale
  - **Dead Stock Indicator**: Products not selling

#### Key Performance Metrics
- **Total Profit**: Sum of all product profits
- **Total Revenue**: Sum of all sales revenue
- **Average Margin**: Average profit margin percentage
- **Average ROI**: Average return on investment
- **Average Sales Velocity**: Overall sales speed
- **Average Stock Turnover**: Overall inventory efficiency
- **Average Performance Score**: Overall product performance
- **Dead Stock Items**: Count of non-selling products

#### Risk Analysis Section
- **Products at Risk**: Items with potential issues
- **Economic Impact**: Potential financial losses
- **Risk Category Distribution**: Expiring vs dead stock
- **Detailed Risk Analysis**: Breakdown by risk type

## Monitoring the System

### Real-time Kafka Producer
- **Logs**: Shows data import every 10 seconds
- **Metrics**: Counts of records sent to each topic
- **Status**: Success/failure indicators

### Spark Streaming Application
- **Logs**: Shows analytics processing every 10 seconds
- **Metrics**: Top products, performance scores, calculations
- **Status**: Processing success/failure indicators

### Admin Interface
- **Auto-refresh**: Data updates automatically every 10 seconds
- **Visualizations**: Charts update with new data
- **Metrics**: All numbers refresh with latest calculations

## Troubleshooting

### Common Issues

#### Kafka Connection Issues
**Symptoms**: 
- "Failed to connect to Kafka"
- "No Kafka brokers available"

**Solutions**:
1. Verify Kafka is running: `ps aux | grep kafka`
2. Check Kafka logs for errors
3. Verify topic names match configuration
4. Check firewall settings if running remotely

#### MongoDB Connection Issues
**Symptoms**:
- "Failed to connect to MongoDB"
- "Authentication failed"

**Solutions**:
1. Verify MongoDB is running: `sudo systemctl status mongod`
2. Check MongoDB logs: `sudo journalctl -u mongod`
3. Verify database name and credentials
4. Check network connectivity

#### Spark Memory Issues
**Symptoms**:
- "Out of memory" errors
- Slow processing

**Solutions**:
1. Increase Spark memory settings
2. Reduce batch size
3. Optimize Spark configurations
4. Monitor memory usage

### Debugging Tips

#### Check Component Logs
```bash
# View real-time producer logs
tail -f /path/to/logs/kafka_producer.log

# View Spark streaming logs
tail -f /path/to/logs/spark_streaming.log

# View Streamlit interface logs
tail -f /path/to/logs/streamlit.log
```

#### Verify Data Flow
```bash
# Check MongoDB collections
mongo stock_management --eval "show collections"

# Check Kafka topics
kafka-console-consumer.sh --topic topic-inventory-updates --bootstrap-server localhost:9092 --from-beginning

# Check Spark processing
# Monitor Spark UI at http://localhost:4040
```

## System Architecture

### Data Flow Diagram
```
┌─────────────┐       ┌─────────────┐       ┌─────────────────┐       ┌─────────────┐       ┌─────────────────┐
│   MongoDB    │──────▶│   Kafka      │──────▶│   Spark          │──────▶│   MongoDB    │──────▶│   Admin          │
│  (Source)    │       │  (Streaming) │       │  (Processing)   │       │  (Results)   │       │  (Interface)     │
└─────────────┘       └─────────────┘       └─────────────────┘       └─────────────┘       └─────────────────┘
       ↑                                                                                           ↑
       │                                                                                           │
       └───────────────────────────────────────────────────────────────────────────────────────┘
                                                                                                 
                                                                                                 
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                                 │
│                        Real-time Analytics System (10s intervals)                          │
│                                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Component Responsibilities

| Component | Responsibility | Frequency |
|-----------|---------------|-----------|
| MongoDB | Store raw and processed data | Continuous |
| Kafka | Stream data between components | Real-time |
| Kafka Producer | Import data from MongoDB to Kafka | Every 10s |
| Spark Streaming | Process analytics and calculations | Every 10s |
| Admin Interface | Display results and visualizations | Real-time |

## Best Practices

### Performance Optimization
1. **Spark Tuning**: Adjust Spark memory and parallelism settings
2. **Data Partitioning**: Optimize MongoDB collections for queries
3. **Batch Sizes**: Balance between real-time and performance
4. **Caching**: Cache frequently accessed data in Spark

### Monitoring
1. **Resource Usage**: Monitor CPU, memory, and disk usage
2. **Data Quality**: Verify data consistency across components
3. **Processing Times**: Track analytics processing duration
4. **Error Rates**: Monitor and alert on processing failures

### Scaling
1. **Horizontal Scaling**: Add more Spark workers for large datasets
2. **Vertical Scaling**: Increase resources for individual components
3. **Load Balancing**: Distribute traffic across multiple instances
4. **High Availability**: Implement redundancy for critical components

## Shutdown Procedure

### Graceful Shutdown
1. **Admin Interface**: Close browser or press Ctrl+C in terminal
2. **Spark Streaming**: Press Ctrl+C in terminal
3. **Kafka Producer**: Press Ctrl+C in terminal
4. **Kafka**: Stop Kafka server and Zookeeper
5. **MongoDB**: Keep running for data persistence

### Emergency Shutdown
```bash
# Kill all Python processes
pkill -f "python.*src"

# Stop Kafka
pkill -f "kafka"
pkill -f "zookeeper"

# MongoDB can remain running for data persistence
```

## Maintenance

### Regular Tasks
1. **Database Backup**: Regular MongoDB backups
2. **Log Rotation**: Manage log file sizes
3. **Dependency Updates**: Keep Python packages updated
4. **Performance Reviews**: Regular system performance checks

### Upgrades
1. **Test in Staging**: Test upgrades in non-production environment
2. **Backup Data**: Always backup before major upgrades
3. **Rollback Plan**: Have a plan to revert if issues occur
4. **Monitor Post-Upgrade**: Watch for performance changes

## Conclusion

This guide provides complete instructions for running the advanced analytics system. The system integrates MongoDB, Kafka, Spark, and Streamlit to provide real-time inventory and sales analytics with sophisticated metrics like sales velocity, stock turnover, dead stock detection, and performance scoring.

For best results:
- Ensure all services are running before starting components
- Monitor logs for any issues
- Start components in the recommended order
- Use the admin interface to view real-time analytics

The system is designed to run continuously, providing up-to-date analytics every 10 seconds for optimal inventory management and sales performance tracking.