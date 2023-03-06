# -------------------

import datetime as dt
import pandas as pd

pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)
pd.set_option('display.float_format', lambda x: '%.2f' % x)

df_ = pd.read_csv("flo_data_20k.csv")
df = df_.copy()


def check_df(dataframe=pd.DataFrame):
    '''
    First view of your Data to understand the concept of your observation units

    Parameters
    ----------
    dataframe : DataFrame

    Returns
    -------

    '''
    print('##############  HEAD ###################')
    print(dataframe.head(10))
    print('##############  SHAPE  #################')
    print(dataframe.shape)
    print('###############  NA  ####################')
    print(dataframe.isnull().sum())
    print('##############  INFO  ###################')
    print(dataframe.info())
    print('#####################################')


check_df(df)


def data_preparation(dataframe=pd.DataFrame):
    '''
        Function that makes the data ready for RFM by preprocessing it.

    Parameters
    ----------
    dataframe : DataFrame
        The dataframe that try to apply RFM analysis.

    Returns
    -------
    rfm : DataFrame
    '''

    dataframe['Total_Order'] = dataframe['order_num_total_ever_online'] + \
                               dataframe['order_num_total_ever_offline']
    dataframe['Total_Price'] = dataframe['customer_value_total_ever_offline'] + \
                               dataframe['customer_value_total_ever_online']
    dataframe = dataframe.astype({col: 'datetime64[ns]'
                                  for col in dataframe.columns
                                  if 'date' in col})

    # Creating Customer ID, Recency, Frequency, Monetary values
    last_invoice_date = dt.datetime.strptime(str(df["last_order_date"].max()), "%Y-%m-%d")
    today_date = last_invoice_date + dt.timedelta(days=2)
    df.info()
    rfm = dataframe.groupby("master_id").agg({'last_order_date': lambda date: (today_date - date.max()).days,
                                              'Total_Order': lambda TotalOrder: TotalOrder,
                                              'Total_Price': lambda TotalPrice: TotalPrice
                                              })

    rfm = rfm.reset_index()
    rfm.columns = ['customer_id', 'recency', 'frequency', 'monetary']
    # rfm.head(10)  # check

    # That gives you calculated RFM Scores
    rfm['recency_score'] = pd.qcut(rfm['recency'], 5, labels=[5, 4, 3, 2, 1])

    # ERROR : Using rank method because of ValueError: Bin edges must be unique
    rfm['frequency_score'] = pd.qcut(rfm['frequency'].rank(method='first'), 5, labels=[1, 2, 3, 4, 5])

    rfm['monetary_score'] = pd.qcut(rfm['monetary'], 5, labels=[1, 2, 3, 4, 5])

    # Expressing recency_score and frequency_score as a single variable and saving it as RF_SCORE
    rfm['RF_SCORE'] = rfm['recency_score'].astype(str) + \
                      rfm['frequency_score'].astype(str)

    # That gives you RF Scores Segmentation
    seg_map = {
        r'[1-2][1-2]': 'hibernating',
        r'[1-2][3-4]': 'at_Risk',
        r'[1-2]5': 'cant_loose',
        r'3[1-2]': 'about_to_sleep',
        r'33': 'need_attention',
        r'[3-4][4-5]': 'loyal_customers',
        r'41': 'promising',
        r'51': 'new_customers',
        r'[4-5][2-3]': 'potential_loyalists',
        r'5[4-5]': 'champions'
    }
    rfm['segment'] = rfm['RF_SCORE'].replace(seg_map, regex=True)

    return rfm


rfm = data_preparation(df)

# TESTING DATA

# FLO includes a new women's shoe brand. The product prices of the brand it includes are
# above the general customer preferences. For this reason, customers in the profile who
# will be interested in the promotion of the brand and product sales are requested to
# be contacted privately.These customers were planned to be loyal, champions and female shoppers.

new_df = pd.DataFrame()
new_df["new_customer_id"] = df[rfm.index.isin(rfm[(rfm["segment"] == "champions")
                                                  | (rfm["segment"] == "loyal_customers")].index)
                               & ((df["interested_in_categories_12"].str.contains("KADIN")))]["master_id"]

new_df.to_csv("new_brand_target_customers_id.cvs")
