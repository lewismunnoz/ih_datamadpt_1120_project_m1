import re

# Let's clean this mess
# Lowercases:
def lower_case(df):
    df['gender'] = df['gender'].str.lower()
    return df

#Gender:
def replace_str(df):
    for i in df['gender']:
        if i.startswith('f') == True:
            df['gender'] = df['gender'].apply(lambda x: re.sub('^f\w+', 'Female',x))
        elif i.startswith('m') == True:
            df['gender'] = df['gender'].apply(lambda x: re.sub('^m\w+', 'Male',x))
    return df


# Concating all functions
def wrangling(df):
    df_lower_case = lower_case(df)
    df_replace_str = replace_str(df_lower_case)

    return df_replace_str