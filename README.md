🚦 Traffic Volume Analysis for Intersection-Level Insights
📌 Project Overview

This project focuses on analyzing traffic volume, public transport delays, and signal timing data to generate actionable insights for improving urban traffic management.

The main goal is to reduce congestion, optimize signal timings, and enhance public transport efficiency using Python, SQL, and Power BI.

🎯 Business Problem

Urban traffic systems often lack visibility into real-time and historical traffic patterns, leading to:

Increased congestion during peak hours

Inefficient signal timings

Public transport delays

Poor traffic management decisions

🎯 Business Objective

Improve traffic flow efficiency at intersections

Reduce congestion during peak hours

Optimize signal timing strategies

📂 Dataset

This project integrates three datasets:

Public Transport Delay Log

Signal Timing Configuration

Traffic Volume Log

These datasets were combined to build a complete traffic analysis system.

🏗️ Project Architecture
Raw CSV Data
     ↓
Python (Data Cleaning & EDA)
     ↓
MySQL Database
     ↓
SQL Analysis
     ↓
Power BI Dashboard

🧠 Python Analysis

Python was used for data preprocessing and exploratory analysis:

🔹 Data Preprocessing

Handling missing values

Removing duplicates

Datatype conversion (timestamps)

Outlier treatment using IQR method

🔹 Exploratory Data Analysis (EDA)

Mean, Median, Mode

Variance & Standard Deviation

Skewness & Kurtosis

Distribution analysis using:

Histogram

Boxplot

Q-Q Plot

🔹 Feature Engineering

Peak hour identification

Delay categorization

Dataset merging for analysis

🗄️ SQL Analysis

Data was stored and analyzed using MySQL:

🔹 Database Operations

Database and table creation

Data loading from CSV files

Data cleaning and transformation

🔹 Analytical Queries

Delay statistics (mean, median, mode)

Peak vs non-peak traffic analysis

Vehicle type distribution

Intersection-level insights

Correlation between traffic and delays

📊 Power BI Dashboard

An interactive dashboard was created to visualize:

Traffic volume trends

Public transport delays

Peak hour congestion

Intersection performance

Vehicle type distribution

📸 Dashboard Preview

![Dashboard](power bi dashboard.png)

📈 Key Insights

🚦 Peak hours show the highest congestion levels

🚌 Delays are mainly caused by signal inefficiencies and vehicle breakdowns

📍 Certain intersections consistently experience higher delays

🚗 Traffic volume strongly impacts transport delays

⚙️ Optimizing signal timing can significantly reduce congestion

🧰 Technologies Used

Python (Pandas, NumPy, Matplotlib, Seaborn)

MySQL

SQLAlchemy

Power BI

🚀 How to Run the Project
# Clone repository
git clone https://github.com/yourusername/Traffic-Volume-Analysis.git

# Install dependencies
pip install pandas numpy matplotlib seaborn sqlalchemy pymysql

# Run Python script
python PYTHON.py

Run SQL script in MySQL:

SQL.sql

Open dashboard:

power bi file.pbix

Support data-driven decision making

📅 Project Workflow

Week 1 → Data Collection & Understanding

Week 2 → Data Cleaning & Preprocessing

Week 3 → Python EDA & SQL Integration

Week 4 → Power BI Dashboard Development

📌 Business Recommendations

Optimize signal timings dynamically

Focus on high-delay intersections

Improve public transport maintenance

Encourage off-peak travel

Monitor KPIs continuously

📌 Tags

Data Analytics | Python | SQL | Power BI | Traffic Analysis | Smart City
