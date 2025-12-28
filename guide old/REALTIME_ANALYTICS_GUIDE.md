# 🚀 Real-Time Analytics System - Complete Guide

## 🎯 Overview

This is a **production-grade real-time analytics system** that implements a **true event-driven architecture** with:

- **Kafka** (Event Streaming)
- **Spark Structured Streaming** (Continuous Processing)
- **MongoDB** (Stateful Analytics Storage)
- **Streamlit** (Admin Interface - Read Only)

## 🔁 Architecture

```
SQLite (Comptable Input)
       ↓
Kafka (Events)
       ↓
Spark Structured Streaming (ALWAYS RUNNING)
       ↓
MongoDB (Hot Analytics Storage)
       ↓
Admin Interface (Streamlit – READ ONLY)
```

## 🚀 How to Run the Complete System

### 1️⃣ Start Spark Streaming (Independent Process)

**Run this FIRST - it must be running continuously:**

```bash
python -m src.analytics.spark_streaming_main
```

**This will:**
- ✅ Start Spark session with Kafka & MongoDB connectors
- ✅ Create continuous streaming queries
- ✅ Wait for Kafka events indefinitely
- ✅ Process events as they arrive
- ✅ Update MongoDB conditionally
- ✅ Log all real-time activity

**Expected Output:**
```
✅ Spark session initialized: RealTimeAnalytics
🚀 Spark streaming started – waiting for Kafka events...
🚀 All Spark streaming queries started - waiting for Kafka events...
🔄 Spark streaming queries running continuously...
```

### 2️⃣ Start Streamlit Admin Interface (Separate Process)

**Run this in a separate terminal:**

```bash
streamlit run src/main.py
```

**This will:**
- ✅ Show the Admin Interface
- ✅ Read ONLY from MongoDB
- ✅ Display last known analytics results
- ✅ Never trigger Spark or Kafka
- ✅ Work even if Spark is processing

### 3️⃣ Generate Test Data (Optional)

To see the system in action, you can:

1. **Use the Accountant Interface** to add manual data
2. **Run the data pipeline** to process Excel files
3. **Use Kafka producers** to send test events

## 🎯 Key Features

### ✅ Continuous Spark Streaming
- **Never exits** - runs indefinitely
- **Event-driven** - processes only when new data arrives
- **Stateful** - MongoDB always has last valid results
- **Conditional writes** - only updates MongoDB when new data exists

### ✅ Admin Interface Behavior
- **Reads ONLY from MongoDB** - completely decoupled
- **Shows last known results** - even if no new data
- **Never triggers Spark** - no interference with streaming
- **Streamlit-safe** - reruns don't affect Spark execution

### ✅ Real-Time Logging
- **Spark Streaming Logs:**
  - `🚀 Spark streaming started – waiting for Kafka events...`
  - `📊 Product winners batch received - X records`
  - `📈 New sales data detected - recalculating product winners`
  - `✅ Product winners successfully written to MongoDB`
  - `📉 No new data - keeping previous results`

- **Admin Interface Logs:**
  - `📊 Admin Interface: Loading product winners from MongoDB...`
  - `📈 Admin Interface: Displaying X product winners`
  - `📉 Admin Interface: No data available - showing placeholder`

## 📊 Analytics Types

### 1️⃣ 🏆 Product Winners
- **Trigger:** New sales events arrive
- **Action:** Recalculate profits, update MongoDB
- **No Data:** Keep previous results
- **MongoDB Collection:** `product_winners`

### 2️⃣ ⚠️ Loss-Risk Products
- **Trigger:** New inventory updates arrive
- **Action:** Detect expiring/dead stock, update MongoDB
- **No Data:** Keep previous results
- **MongoDB Collection:** `loss_risk_products`

### 3️⃣ 💸 Client Promotions
- **Trigger:** New sales data arrives
- **Action:** Update revenue, calculate eligibility
- **No Data:** Keep previous client states
- **MongoDB Collection:** `clients`

## 🛑 Shutdown Procedure

### Stop Spark Streaming Gracefully
```bash
# In the Spark terminal, press Ctrl+C
# This will trigger graceful shutdown:
🛑 Received keyboard interrupt, shutting down...
🛑 Stopping X streaming queries...
✅ Stopped streaming query: query_id
🔥 Stopping Spark session...
✅ Spark session stopped successfully
✅ Application exit complete
```

### Stop Streamlit Interface
```bash
# In the Streamlit terminal, press Ctrl+C
```

## 🔧 Configuration

All configuration is in `src/utils/config.py`:

- **Kafka:** `KAFKA_BOOTSTRAP_SERVERS`, topic names
- **MongoDB:** `SPARK_MONGODB_URI`, database names
- **Spark:** `SPARK_APP_NAME`, `SPARK_JARS_PACKAGES`
- **Business Rules:** `REVENUE_THRESHOLD_PROMO`

## 📋 Troubleshooting

### ❌ "No data showing in Admin Interface"
- **Check:** Is Spark streaming running?
- **Check:** Are there events in Kafka?
- **Check:** Does MongoDB have data?
- **Solution:** Add test data via Accountant Interface

### ❌ "Spark exits immediately"
- **Check:** Are you running the standalone version?
- **Solution:** Use `python -m src.analytics.spark_streaming_main`

### ❌ "Admin Interface keeps reloading"
- **This is normal** - Streamlit reruns don't affect Spark
- **Data persists** - MongoDB maintains state between reruns

## 🎓 Architecture Principles

### 1️⃣ **Separation of Concerns**
- **Spark:** Continuous processing only
- **Streamlit:** Display only
- **MongoDB:** State management only

### 2️⃣ **Event-Driven Design**
- Process **only** when new data arrives
- Preserve state when no new data
- Never overwrite valid results

### 3️⃣ **Stateful Analytics**
- MongoDB = source of truth
- Always contains last valid state
- Admin interface reads from MongoDB only

### 4️⃣ **Production-Grade**
- Graceful shutdown handling
- Comprehensive error handling
- Detailed logging
- Configurable parameters

## 📚 Usage Scenarios

### Scenario 1: Real-Time Sales Analytics
```
1. Accountant adds sale via UI
2. Data goes to SQLite → Kafka
3. Spark detects new sales event
4. Spark recalculates product winners
5. Spark updates MongoDB
6. Admin Interface shows updated results
```

### Scenario 2: No New Data
```
1. No new Kafka events
2. Spark continues running
3. MongoDB keeps last results
4. Admin Interface shows last results
5. No data loss, no empty screens
```

### Scenario 3: System Recovery
```
1. Spark restarts after crash
2. Reads from checkpoint locations
3. Continues from last processed event
4. MongoDB has all historical data
5. Admin Interface shows complete history
```

## 🎯 Production Deployment

For production use:

1. **Run Spark as a service:**
   ```bash
   nohup python -m src.analytics.spark_streaming_main > spark.log 2>&1 &
   ```

2. **Run Streamlit as a service:**
   ```bash
   nohup streamlit run src/main.py > streamlit.log 2>&1 &
   ```

3. **Monitor logs:**
   ```bash
   tail -f spark.log
   tail -f streamlit.log
   ```

4. **Use process managers:**
   - Systemd
   - Supervisor
   - Docker

## ✅ Checklist for Successful Operation

- [ ] Spark streaming process is running continuously
- [ ] Streamlit admin interface is accessible
- [ ] Kafka has the correct topics (sales, inventory, clients)
- [ ] MongoDB is accessible and has proper collections
- [ ] Logs show "Spark streaming queries running continuously..."
- [ ] Admin interface shows data or appropriate placeholders
- [ ] No errors in either Spark or Streamlit logs

## 📊 Expected Behavior

| Situation | Spark Behavior | MongoDB Behavior | Admin Interface Behavior |
|-----------|---------------|------------------|--------------------------|
| New sales event | Processes immediately | Updates product_winners | Shows updated results |
| No new events | Keeps running | Maintains last state | Shows last results |
| Inventory update | Processes immediately | Updates loss_risk_products | Shows updated results |
| Spark restart | Continues from checkpoint | Preserves all data | Shows complete history |
| Streamlit rerun | No effect | No effect | Shows current MongoDB state |

This is a **true real-time, event-driven, stateful analytics system** suitable for Master/PFE Big Data projects. 🎉