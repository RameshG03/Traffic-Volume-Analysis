# -*- coding: utf-8 -*-
"""
Created on Fri Aug 29 21:06:38 2025

@author: gkart
"""

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import scipy.stats as stats
import warnings
warnings.filterwarnings("ignore")

# Load Dataset 

St_df=pd.read_csv(r"C:\DATA ANALYSIS\PROJECT-_284\Dataset\REAL DATASET\Signal_Timing_Config.csv")
Tv_df=pd.read_csv(r"C:\DATA ANALYSIS\PROJECT-_284\Dataset\REAL DATASET\Traffic_Volume_Log.csv")
Pt_df=pd.read_csv(r"C:\DATA ANALYSIS\PROJECT-_284\Dataset\REAL DATASET\Public_Transport_Delay_Log.csv")

#  Initial EDA

St_df.info()
St_df.describe()
St_df.isnull().sum()

Tv_df.info
Tv_df.describe()
Tv_df.isnull().sum()

Pt_df.info
Pt_df.describe()
Pt_df.isnull().sum()

# check data type

St_df.dtypes
Tv_df.dtypes
Pt_df.dtypes

# Convert datatype

St_df["effective_start_ts"] = pd.to_datetime(St_df["effective_start_ts"])
St_df["effective_end_ts"] = pd.to_datetime(St_df["effective_end_ts"])

Tv_df["timestamp_utc"] = pd.to_datetime(Tv_df["timestamp_utc"])
Tv_df['date'] = pd.to_datetime(Tv_df['date'], format='%d-%m-%Y',errors='coerce')

Pt_df["scheduled_arrival_ts"] = pd.to_datetime(Pt_df["scheduled_arrival_ts"])
Pt_df["actual_arrival_ts"] = pd.to_datetime(Pt_df["actual_arrival_ts"])
Pt_df['date'] = pd.to_datetime(Pt_df['date'], format='%d-%m-%Y',errors='coerce')

# after change data type

St_df.dtypes
Tv_df.dtypes
Pt_df.dtypes

#  Function to determine the scale of measurement
def determine_scale_of_measurement(series):
    if pd.api.types.is_bool_dtype(series):
        return 'Nominal'
    elif series.dtype == 'object' or series.dtype.name == 'category':
        if series.nunique() < 10:
            return 'Ordinal'   # Ex: Shift: Morning, Evening, Night
        else:
            return 'Nominal'   # Ex: Gender, Diagnosis
    elif pd.api.types.is_numeric_dtype(series):
        if (series.min() >= 0) and (series.dropna() % 1 == 0).all():
            return 'Ratio'     # Ex: Age, Bed ID, Used Units
        else:
            return 'Interval'  # Ex: Scores without true zero
    else:
        return 'Unknown'

#  Function to create data dictionary for one table
def create_data_dictionary(df, table_name):
    return pd.DataFrame({
        'Table': table_name,
        'Column Name': df.columns,
        'Data Type': [df[col].dtype for col in df.columns],
        'Scale of Measurement': [determine_scale_of_measurement(df[col]) for col in df.columns]
    })

#  Create dictionary for each table
dict_Pt = create_data_dictionary(Pt_df, 'Pt')
dict_St    = create_data_dictionary(St_df, 'St')
dict_Tv   = create_data_dictionary(Tv_df, 'Tv')

#  Combine into one final dictionary
final_dict = pd.concat([dict_Pt, dict_St, dict_Tv], ignore_index=True)

#  Display or export
print(final_dict)



# calculate mean values for all numeric columns:

St_mean = St_df.mean(numeric_only=True)
Tv_mean = Tv_df.mean(numeric_only=True)
Pt_mean= Pt_df.mean(numeric_only=True)

St_mean
Tv_mean
Pt_mean

# calculate median values for all numeric columns:

St_median = St_df.median(numeric_only=True)
Tv_median = Tv_df.median(numeric_only=True)
Pt_median = Pt_df.median(numeric_only=True)

St_median
Tv_median
Pt_median

# calculate mode values for all numeric colums:

St_mode = Pt_df.mode(numeric_only=True)
Tv_mode = Tv_df.mode(numeric_only=True)
Pt_mode = Pt_df.mode(numeric_only=True)

St_mode
Tv_mode
Pt_mode

# Calculate Range for all numeric columns:

St_range = St_df.select_dtypes(include='number').max()
Tv_range = Tv_df.select_dtypes(include='number').max()
Pt_range = Pt_df.select_dtypes(include='number').max()

St_range
Tv_range
Pt_range

# Calculate variance of all numeric columns

St_variance = St_df.select_dtypes(include='number').var()
Tv_variance = Tv_df.select_dtypes(include='number').var()
Pt_variance = Pt_df.select_dtypes(include='number').var()

St_variance
Tv_variance
Pt_variance

# Calculate standard deviation for all numeric columns:

St_std = St_df.std(numeric_only=True)
Tv_std = Tv_df.std(numeric_only=True)
Pt_std = Pt_df.std(numeric_only=True)

St_std
Tv_std
Pt_std

# Calculate skewness for all numeric columns:

St_skew = St_df.skew(numeric_only=True)
Tv_skew = Tv_df.skew(numeric_only=True)
Pt_skew = Pt_df.skew(numeric_only=True)

St_skew
Tv_skew
Pt_skew

# Calculate kurtosis for all numeric columns:
    
St_kurtosis = St_df.kurt(numeric_only=True)
Tv_kurtosis = Tv_df.kurt(numeric_only=True)
Pt_kurtosis = Pt_df.kurt(numeric_only=True)

St_kurtosis
Tv_kurtosis
Pt_kurtosis

# Box plot Pt_df

plt.subplot(3, 1,  2)
sns.boxplot(x=Pt_df["delay_minutes"])
plt.title("Boxplot - delay_minutes")
plt.show()

# Box plot Tv_df

plt.subplot(3, 1, 2)
sns.boxplot(x=Tv_df["vehicle_count_total"])
plt.title("Boxplot - vehicle_count_total")
plt.show()

# Box plot St_df

plt.subplot(3, 1, 1)
sns.boxplot(x=St_df["duration_sec"])
plt.title("Boxplot - duration_sec")
plt.show()

# Data preprocesssing (outlier handling and Transformation)
# IQR

Q1 = St_df["duration_sec"].quantile(0.25)
Q3 = St_df["duration_sec"].quantile(0.75)

IQR = Q3-Q1
lower_bound = Q1-1.5*IQR
upper_bound = Q3 + 1.5*IQR
St_df["duration_sec_capped"] = St_df["duration_sec"].clip(lower=lower_bound, upper=upper_bound)

Q1 = Tv_df["vehicle_count_total"].quantile(0.25)
Q3 = Tv_df["vehicle_count_total"].quantile(0.75)
IQR = Q3-Q1
lower_bound = Q1-1.5*IQR
upper_bound = Q3 + 1.5*IQR
Tv_df["vehicle_count_capped"] = Tv_df["vehicle_count_total"].clip(lower_bound, upper= upper_bound)

Q1 = Pt_df["delay_minutes"].quantile(0.25)
Q3 = Pt_df["delay_minutes"].quantile(0.75)

IQR = Q3-Q1
lower_bound = Q1 -1.5 * IQR
upper_bound = Q3+ 1.5 * IQR
Pt_df["delay_minutes_capped"] = Pt_df["delay_minutes"].clip(lower_bound, upper = upper_bound)

#  Visualizations - SIGNAL_TIME_CONFIG
# Histogram plot:
    
plt.figure(figsize=(12, 4))
plt.subplot(5, 4, 2)
St_df['duration_sec'].hist(bins=20)
plt.title('Histogram - duration_sec')
plt.show()

# Boxplot:
    
plt.subplot(3, 1,  1)
sns.boxplot(x=St_df['duration_sec'])
plt.title('Boxplot - duration_sec')
plt.show()

# Q-Q plot:
    
plt.subplot(2, 2, 3)
stats.probplot(St_df['duration_sec'], dist="norm", plot=plt)
plt.title('Q-Q Plot - duration_sec')
plt.tight_layout()
plt.show()

# Visualizations - TRAFFIC_VOLUME_LOG
# Histogram plot:
    
plt.figure(figsize=(8, 17))
plt.subplot(5, 4, 5)
Tv_df['vehicle_count_total'].hist(bins=20)
plt.title('Histogram - vehicle_count_total')
plt.show()

# Box plot Tv_df

plt.subplot(3, 1, 2)
sns.boxplot(x=Tv_df["vehicle_count_total"])
plt.title("Boxplot - vehicle_count_total")
plt.show()

# Q-Q plot:
    
plt.subplot(2, 2, 3)
stats.probplot(Tv_df['vehicle_count_total'], dist="norm", plot=plt)
plt.title('Q-Q Plot - vehicle_count_total')
plt.tight_layout()
plt.show()

#  Visualizations - PUBLIC_TRANSPORT_DELAY
# Histogram plot:
    
plt.figure(figsize=(12, 10))
plt.subplot(5, 4, 5)
Pt_df['delay_minutes'].hist(bins=20)
plt.title('Histogram - delay_minutes')
plt.show()


# Box plot Pt_df

plt.subplot(3, 1,  2)
sns.boxplot(x=Pt_df["delay_minutes"])
plt.title("Boxplot - delay_minutes")
plt.show()

# Q-Q plot:
    
plt.subplot(2, 2, 3)
stats.probplot(Pt_df["delay_minutes"], dist="norm", plot=plt)
plt.title('Q-Q Plot - delay_minutes')
plt.tight_layout()
plt.show()


------------------------------------------------------------------------------------------------------
# sql

import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote

# Load CSV files

St_df=pd.read_csv(r"C:\DATA ANALYSIS\PROJECT-_284\Dataset\REAL DATASET\Signal_Timing_Config.csv")
Tv_df=pd.read_csv(r"C:\DATA ANALYSIS\PROJECT-_284\Dataset\REAL DATASET\Traffic_Volume_Log.csv")
Pt_df=pd.read_csv(r"C:\DATA ANALYSIS\PROJECT-_284\Dataset\REAL DATASET\Public_Transport_Delay_Log.csv")


# Connection details
user = 'user70'
pw = 'user70'
port='3306'
db = 'Traffic_volum_db'

# Create engine (for SQLAlchemy < 2.x)
encoded_pw = quote(pw)
engine = create_engine(f"mysql+pymysql://{user}:{encoded_pw}@localhost/{db}")

# Save to separate tables
St_df.to_sql('signal_config', con=engine, if_exists='replace', index=False)
Tv_df.to_sql('traffic_volume', con=engine, if_exists='replace', index=False)
Pt_df.to_sql('delay_log', con=engine, if_exists='replace', index=False)

# Read back from MySQL
St = "SELECT * FROM Signal_config"
Tv = "SELECT * FROM traffic_volume"
Pt = "SELECT * FROM delay_log"

# Use text() for SQLAlchemy 2.x compatibility
from sqlalchemy import text
St_df_sql = pd.read_sql_query(text("SELECT * FROM signal_config"), engine.connect())
Tv_df_sql = pd.read_sql_query(text("SELECT * FROM signal_config"), engine.connect())
Pt_df_sql = pd.read_sql_query(text("SELECT * FROM delay_log"), engine.connect())
