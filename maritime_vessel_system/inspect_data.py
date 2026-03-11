#!/usr/bin/env python3
"""Inspect the source CSV dataset to understand its structure"""

import pandas as pd
import json

df = pd.read_csv('case_study_dataset_202509152039.csv')

print('='*80)
print('DATASET STRUCTURE')
print('='*80)
print(f'Total rows: {len(df)}')
print(f'Total columns: {len(df.columns)}')
print(f'\nColumns: {list(df.columns)}')

print('\n' + '='*80)
print('SAMPLE ROW #1')
print('='*80)
sample = df.iloc[0].to_dict()
for key, val in sample.items():
    print(f'{key}: {val} ({type(val).__name__})')

print('\n' + '='*80)
print('MISSING VALUES (null counts)')
print('='*80)
missing = df.isnull().sum()
missing = missing[missing > 0].sort_values(ascending=False)
if len(missing) > 0:
    for col, count in missing.items():
        pct = (count / len(df) * 100)
        print(f'{col}: {count} ({pct:.1f}%)')
else:
    print('No null values found!')

print('\n' + '='*80)
print('COLUMN DATA TYPES')
print('='*80)
print(df.dtypes)

print('\n' + '='*80)
print('UNIQUE VESSEL TYPES')
print('='*80)
if 'vessel_type' in df.columns:
    print(df['vessel_type'].value_counts())
elif 'vesselType' in df.columns:
    print(df['vesselType'].value_counts())
else:
    print('No vessel_type column found')

print('\n' + '='*80)
print('FLAG/COUNTRY DISTRIBUTION')
print('='*80)
if 'flag' in df.columns:
    print(df['flag'].value_counts().head(15))
else:
    print('No flag column found')

print('\n' + '='*80)
print('NUMERIC RANGES')
print('='*80)
numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
for col in numeric_cols:
    non_null = df[col][df[col].notna()]
    if len(non_null) > 0:
        print(f'{col}: {non_null.min():.2f} to {non_null.max():.2f} (avg: {non_null.mean():.2f})')

print('\n' + '='*80)
print('CATEGORICAL VALUES' )
print('='*80)
categorical_cols = df.select_dtypes(include=['object']).columns
for col in categorical_cols[:5]:  # First 5 only
    unique_count = df[col].nunique()
    print(f'{col}: {unique_count} unique values')
    if unique_count <= 20:
        print(f'  Values: {df[col].unique().tolist()}')
