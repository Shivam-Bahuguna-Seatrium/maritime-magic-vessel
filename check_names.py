import pandas as pd

df = pd.read_csv('case_study_dataset_202509152039.csv')
print(f'Total rows: {len(df)}')
print(f'Non-null names: {df["name"].notna().sum()}')
print(f'Unique names: {df["name"].nunique()}')
print('Sample names:')
print(df['name'].dropna().head(20).tolist())
print('\nNull name count:', df['name'].isna().sum())
print('\nEmpty string count:', (df['name'] == '').sum())
