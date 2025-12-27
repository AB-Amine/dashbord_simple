# Real-Time Economic Analytics Platform

This project is a real-time data analytics platform designed to process sales, inventory, and client data. It follows a Lambda-style architecture using Kafka for data ingestion, Spark Streaming for processing, and MongoDB for storage, with a Streamlit web interface for data upload and visualization.

## Architecture

The data flows through the system as follows:

1.  **Data Ingestion**: An "Accountant" uploads an Excel file (`stock_data.xlsx`) through the Streamlit web application.
2.  **Queuing**: The Python Kafka producer (`kafka_producer.py`) parses the Excel file and sends records to specific Kafka topics.
3.  **Stream Processing**: A Spark Streaming job (`spark_processor.py`) consumes the data from Kafka in real-time. It performs analytics such as calculating top-performing products, identifying stock at risk, and updating client loyalty status.
4.  **Storage**:
    *   **Hot Storage (MongoDB)**: The results of the real-time analysis are written to a MongoDB database for immediate use by the dashboard.
    *   **Cold Storage (Filesystem)**: Raw data from the Kafka streams is continuously saved to the local filesystem in Parquet format (simulating a Data Lake like HDFS).
5.  **Visualization**: An "Administrator" can view the real-time analytics on a dashboard within the Streamlit application, which reads its data from MongoDB.

## How to Run

1.  **Start Infrastructure**: Launch the backend services (Kafka, Zookeeper, MongoDB).
    ```sh
    sudo docker-compose up -d
    ```
2.  **Start the Spark Processor**: Run the Spark job in the background to start processing data.
    ```sh
    source venv/bin/activate
    python spark_processor.py &
    ```
3.  **Start the Web Application**: Launch the Streamlit frontend.
    ```sh
    source venv/bin/activate
    streamlit run app.py
    ```
4.  **Use the Application**:
    *   Navigate to the Streamlit URL in your browser.
    *   In the "Accountant" view, upload the `stock_data.xlsx` file.
    *   Switch to the "Administrator" view to see the analytics update in near real-time.

## File Functions

*   `app.py`: The main Streamlit web application. It provides the UI for both the "Accountant" (file upload) and "Administrator" (analytics dashboard) roles.
*   `spark_processor.py`: The core of the analytics pipeline. This Spark Streaming application connects to Kafka, consumes data streams, performs calculations, and writes results to MongoDB and the local filesystem.
*   `kafka_producer.py`: A Python script responsible for reading the `stock_data.xlsx` file, transforming its contents into JSON messages, and producing them to the correct Kafka topics.
*   `docker-compose.yml`: Defines and configures the project's infrastructure services: `kafka`, `zookeeper`, and `mongodb`.
*   `requirements.txt`: A list of all the Python packages required to run the project.
*   `stock_data.xlsx`: The sample input data file containing sheets for Products, Clients, and Invoices.
*   `database.py`, `analytics.py`, `stock_management.db`: **(Deprecated)** These are legacy files from a previous, SQLite-based architecture and are no longer used by the application.

## Database Structure (MongoDB)

The `stock_management` database is used as the hot storage layer. It contains the following collections, which are populated by the Spark job:

*   `inventory`: Stores product master data.
    *   `product_id`, `name`, `quantity`, `buy_price`, `sell_price`, etc.
*   `clients`: Stores client master data. This collection is updated with analytics results.
    *   `client_id`, `name`, `total_revenue_generated`, `loyalty_tier`, `eligible_promo`
*   `product_winners`: Contains the top 10 most profitable products. This collection is completely overwritten with each new calculation.
    *   `product_id`, `name`, `total_profit`
*   `loss_risk_products`: A list of products identified as being at risk of loss (e.g., expiring soon or dead stock).
    *   `product_id`, `name`, `expiry_date`, `last_movement`, etc.

## Kafka Topics

*   `topic-inventory-updates`: Carries messages related to product creation and updates.
*   `topic-clients`: Carries messages related to client master data.
*   `topic-raw-sales`: Carries messages containing detailed sales transaction data.
