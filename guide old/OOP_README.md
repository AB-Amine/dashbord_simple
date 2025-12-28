# Real-Time Economic Analytics Platform - OOP Version

This is an object-oriented refactoring of the Real-Time Economic Analytics Platform. The application has been transformed from procedural code to a modular, class-based architecture.

## Directory Structure

```
src/
├── main/                  # Main application classes
│   ├── application.py     # Main application orchestrator
│   └── __init__.py
│
├── interface/             # User interface components
│   ├── user_interface.py  # Streamlit UI for accountants and administrators
│   └── __init__.py
│
├── data/                  # Data persistence layer
│   ├── data_manager.py    # SQLite database operations
│   └── __init__.py
│
├── kafka/                 # Kafka integration
│   ├── kafka_producer.py  # Kafka producer for data transfer
│   └── __init__.py
│
├── analytics/             # Analytics processing
│   ├── spark_analytics.py # Spark streaming analytics
│   └── __init__.py
│
├── utils/                 # Utility classes
│   ├── config.py          # Configuration management
│   └── __init__.py
│
├── main.py                # Main entry point
└── __init__.py
```

## Key Classes

### 1. `RealTimeAnalyticsApplication` (Main Orchestrator)
- **Location**: `src/main/application.py`
- **Responsibility**: Orchestrates all system components
- **Key Methods**:
  - `initialize_system()`: Initializes database, Kafka, and Spark
  - `run()`: Starts the user interface
  - `process_data_file()`: Processes data through the entire pipeline
  - `shutdown()`: Cleanly shuts down all components

### 2. `Config` (Configuration Management)
- **Location**: `src/utils/config.py`
- **Responsibility**: Centralizes all application configuration
- **Features**:
  - Database configurations (SQLite, MongoDB)
  - Kafka broker and topic settings
  - Spark session configurations
  - Environment variable support

### 3. `DataManager` (Data Persistence)
- **Location**: `src/data/data_manager.py`
- **Responsibility**: SQLite database operations
- **Key Methods**:
  - `initialize_database()`: Creates database schema
  - `load_excel_data()`: Imports data from Excel files
  - `get_products()`, `get_clients()`, etc.: Data retrieval methods

### 4. `KafkaProducer` (Data Transfer)
- **Location**: `src/kafka/kafka_producer.py`
- **Responsibility**: Data transfer from SQLite to Kafka
- **Key Methods**:
  - `transfer_data_from_sqlite()`: Transfers data to Kafka topics
  - `produce_data()`: Sends records to specific Kafka topics
  - `initialize_topics()`: Ensures Kafka topics are available

### 5. `SparkAnalytics` (Stream Processing)
- **Location**: `src/analytics/spark_analytics.py`
- **Responsibility**: Real-time analytics processing
- **Key Methods**:
  - `process_streams()`: Processes Kafka streams with Spark
  - `initialize_spark()`: Sets up Spark session
  - Analytics functions for product winners, loss detection, client promotions

### 6. `UserInterface` (User Interaction)
- **Location**: `src/interface/user_interface.py`
- **Responsibility**: Streamlit-based user interface
- **Key Methods**:
  - `run()`: Starts the main UI
  - `show_accountant_view()`: Data ingestion interface
  - `show_administrator_view()`: Analytics dashboard

## Data Flow

1. **Data Ingestion**: Excel files → SQLite (via `DataManager`)
2. **Data Transfer**: SQLite → Kafka (via `KafkaProducer`)
3. **Stream Processing**: Kafka → Spark Analytics → MongoDB (via `SparkAnalytics`)
4. **Visualization**: MongoDB → Streamlit UI (via `UserInterface`)

## Running the Application

### Prerequisites
- Python 3.8+
- Docker (for Kafka, MongoDB)
- Java (for Spark)

### Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Or install as a package
pip install -e .
```

### Starting Services

```bash
# Start Kafka and MongoDB
docker-compose up -d

# Wait for services to be ready
sleep 30
```

### Running the Application

```bash
# Run the main application
python src/main.py

# Or use the installed command
analytics-platform
```

### Running Individual Components

```bash
# Run Spark analytics processor
python src/analytics/spark_analytics.py

# Run Kafka producer (for testing)
python src/kafka/kafka_producer.py
```

## Configuration

The application uses environment variables for configuration. You can override default settings by setting environment variables:

```bash
export DB_NAME="my_database"
export MONGO_URI="mongodb://my-mongo:27017/"
export KAFKA_BOOTSTRAP_SERVERS="my-kafka:9092"
```

## Testing

The application includes comprehensive error handling and logging. To test:

1. Upload an Excel file through the Accountant interface
2. Monitor the console for processing status
3. Switch to Administrator view to see analytics results
4. Check MongoDB collections for processed data

## Benefits of OOP Architecture

1. **Modularity**: Each component is isolated and can be developed independently
2. **Reusability**: Classes can be reused in other applications
3. **Maintainability**: Clear separation of concerns makes the code easier to maintain
4. **Testability**: Individual components can be tested in isolation
5. **Scalability**: New features can be added without affecting existing code
6. **Error Handling**: Centralized error handling in each class

## Migration Notes

The OOP version maintains full compatibility with the original functionality while providing:
- Better organization through classes and modules
- Improved error handling and logging
- Configuration management through a central Config class
- Clean separation between data, processing, and presentation layers
- Proper resource management with shutdown methods

## Future Enhancements

1. Add unit tests for each class
2. Implement logging instead of print statements
3. Add more configuration options
4. Implement data validation
5. Add monitoring and metrics
6. Implement proper exception hierarchy