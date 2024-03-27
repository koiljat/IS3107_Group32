import pandas as pd

df1 = pd.read_csv('../Datasets/sgcarmart_v2.csv') 
df2 = pd.read_csv('../Datasets/res_transformed.csv')


df2.columns = df2.columns.str.strip()
df1.columns = df1.columns.str.strip()

df1.drop('new_name', axis=1, inplace = True)

df2.rename(columns= {
    'capacity' : 'eng_cap',
    'milleage' : 'mileage',
    'registration_date' : 'reg_date',
},inplace=True)

df1.rename(columns={
    'name': 'model',
    'owners' : 'no_of_owner',
}, inplace=True)

print(df2.columns)
print(df1.columns)

# Merge the two datasets on the common columns
# We use an outer join to ensure all data is included from both datasets
merged_df = pd.merge(df1, df2, how='outer')


# Save the merged dataset to a new CSV file
merged_df.to_csv('../Dataset/merged_dataset.csv', index=False)
