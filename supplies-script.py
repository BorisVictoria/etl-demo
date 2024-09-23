import pandas as pd

# rm_main is a mandatory function, 
# the number of arguments has to be the number of input ports (can be none),
#     or the number of input ports plus one if "use macros" parameter is set
# if you want to use macros, use this instead and check "use macros" parameter:
#def rm_main(data,macros):
def rm_main(data):
    df = data
    df.info()
    table1 = df[['_id.$oid', 'saleDate.$date', 'storeLocation', 'customer.email', 'customer.gender', 'customer.satisfaction', 'couponUsed', 'purchaseMethod']]
    table1.columns.name = None
    table1.columns = ['id', 'sale_date', 'store_location', 'customer_email', 'customer_gender', 'customer_satisfaction', 'coupon_used', 'purchase_method']
    table1['sale_date'] = pd.to_datetime(table1['sale_date'], unit='ms')
    table1['coupon_used'] = table1['coupon_used'].replace({'true': 1, 'false': 0}) #convert boolean to bits
	
    table2 = df

    table2= table2.melt(id_vars=['_id.$oid'],
                        value_vars=[col for col in table2.columns if col != '_id.$oid'],
                        var_name='item_info',
                        value_name='value')

    table2['item'] = table2['item_info'].str.extract(r'items\[(\d+)\]')
    table2['item'] = pd.to_numeric(table2['item'])
    table2['attribute'] = table2['item_info'].str.extract(r'items\[\d+\]\.(.*)')

    table2 = table2.pivot_table(index=['_id.$oid', 'item'],
                                columns='attribute',
                                values='value',
                                aggfunc='first').reset_index()

    table2 = table2.drop(columns=['item'])
    table2['supplies_orders_items_key'] = table2.index


    table2_final = table2.drop(columns=['tags[0]', 'tags[1]', 'tags[2]', 'tags[3]'])


    #set proper names and use proper datatypes
    table2_final.columns.name = None
    table2_final.columns = ['order_id', 'name', 'price', 'quantity', 'supplies_orders_items_key']
    table2_final['quantity'] = pd.to_numeric(table2_final['quantity'])
    table2_final['price'] = pd.to_numeric(table2_final['price'])


    table3 = table2.melt(id_vars=['supplies_orders_items_key'],
                         value_vars=['tags[0]', 'tags[1]', 'tags[2]', 'tags[3]'],
                         var_name="tag_index",
                         value_name="tag_name")

    #drop nulls
    table3 = table3.dropna(axis=0)
    table3 = table3.drop(columns=['tag_index'])
    table3['supplies_orders_items_tags_key'] = table3.index



    # connect 2 output ports to see the results
    return table1, table2_final, table3