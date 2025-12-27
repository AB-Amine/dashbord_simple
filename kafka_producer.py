import json
import pandas as pd
from confluent_kafka import Producer
import socket
import numpy as np

def acked(err, msg):
    """Delivery report callback called on producing a message."""
    if err is not None:
        print(f"Failed to deliver message: {err}")
    else:
        print(f"Message produced to {msg.topic()} [{msg.partition()}]")

def get_kafka_producer():
    """Creates and returns a Confluent Kafka producer."""
    conf = {'bootstrap.servers': 'localhost:9092', 'client.id': socket.gethostname()}
    return Producer(conf)

def convert_types(obj):
    """ Helper function to convert numpy types to native python types """
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    elif isinstance(obj, list):
        return [convert_types(item) for item in obj]
    elif isinstance(obj, dict):
        return {k: convert_types(v) for k, v in obj.items()}
    else:
        return obj

def produce_data(producer, topic, records):
    """
    Sends a list of records to a Kafka topic.
    """
    for record in records:
        try:
            # Convert numpy/pandas types to native python types for JSON serialization
            converted_record = convert_types(record)
            
            producer.produce(topic, key=None, value=json.dumps(converted_record).encode('utf-8'), callback=acked)
            producer.poll(0)
        except Exception as e:
            print(f"Error sending message to Kafka: {e}")

def main(excel_file='stock_data.xlsx'):
    """
    Main function to read excel data and produce it to Kafka.
    """
    producer = get_kafka_producer()

    # --- Process Inventory ---
    try:
        inventory_df = pd.read_excel(excel_file, sheet_name='Products')
        inventory_df = inventory_df.rename(columns={'id': 'product_id', 'price': 'sell_price'})
        produce_data(producer, 'topic-inventory-updates', inventory_df.to_dict('records'))
    except Exception as e:
        print(f"Could not process 'Products' sheet. Error: {e}")

    # --- Process Clients ---
    try:
        clients_df = pd.read_excel(excel_file, sheet_name='Clients')
        clients_df = clients_df.rename(columns={'id': 'client_id'})
        produce_data(producer, 'topic-clients', clients_df.to_dict('records'))
    except Exception as e:
        print(f"Could not process 'Clients' sheet. Error: {e}")

    # --- Process Sales ---
    try:
        invoices_df = pd.read_excel(excel_file, sheet_name='Invoices')
        invoice_details_df = pd.read_excel(excel_file, sheet_name='Invoice_Details')

        # Prepare details to be nested
        invoice_details_df['items'] = invoice_details_df.apply(
            lambda row: {'product_id': row['product_id'], 'quantity': row['quantity']}, axis=1
        )
        
        grouped_details = invoice_details_df.groupby('facture_id')['items'].apply(list).reset_index()
        invoices_df['id'] = invoices_df['id'].astype(int)
        grouped_details['facture_id'] = grouped_details['facture_id'].astype(int)

        sales_df = pd.merge(invoices_df, grouped_details, left_on='id', right_on='facture_id')
        
        sales_df = sales_df.rename(columns={
            'id': 'invoice_id',
            'date': 'timestamp',
            'total': 'total_amount'
        })
        
        sales_records = sales_df[['invoice_id', 'client_id', 'timestamp', 'total_amount', 'items']].to_dict('records')
        produce_data(producer, 'topic-raw-sales', sales_records)

    except Exception as e:
        print(f"Could not process 'Invoices' and 'Invoice_Details' sheets. Error: {e}")

    producer.flush()

if __name__ == '__main__':
    main()