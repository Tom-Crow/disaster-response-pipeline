import pandas as pd
from sqlalchemy import create_engine
import sys


def load_data(messages_filepath, categories_filepath):
    """
    :param messages_filepath: path to messages csv
    :param categories_filepath: path to categories csv
    :return: DataFrame containing messages and categories data
    """

    messages = pd.read_csv(messages_filepath)
    categories = pd.read_csv(categories_filepath)

    expanded_categories = expand_categories_data(categories)

    # Join messages and expanded_categories dataframes
    df = pd.concat([messages, expanded_categories], axis=1)

    return df


def expand_categories_data(cat_df):
    """
    :param cat_df: categories DataFrame
    :return: DataFrame which has been cleaned and expanded with one-hot encoded variables
    """
    cat_df = cat_df['categories'].str.split(";", expand=True)

    # Find column names for expanded df
    row = cat_df.iloc[0, :]
    category_col_names = list(row.apply(lambda x: x.split("-")[0]))
    cat_df.columns = category_col_names

    # Convert category values to Ones or Zeroes
    for col in cat_df:
        cat_df[col] = cat_df[col].apply(lambda x: x[-1]).astype(int)

    return cat_df


def clean_data(df):
    """
    :param df: DataFrame of messages and categories
    :return: cleaned DataFrame of messages and categories, with duplicates removed
    """

    return df.drop_duplicates(subset='id')


def save_data(df, database_filename):
    """

    :param df: DataFrame with messages and categories
    :param database_filename: name for database
    :return: -
    """
    engine = create_engine('sqlite:///' + database_filename)
    df.to_sql('disaster-messages', engine, index=False)


def main():
    if len(sys.argv) == 4:

        messages_filepath, categories_filepath, database_filepath = sys.argv[1:]

        print('Loading data...\n    MESSAGES: {}\n    CATEGORIES: {}'
              .format(messages_filepath, categories_filepath))
        df = load_data(messages_filepath, categories_filepath)

        print('Cleaning data...')
        df = clean_data(df)
        
        print('Saving data...\n    DATABASE: {}'.format(database_filepath))
        save_data(df, database_filepath)
        
        print('Cleaned data saved to database!')
    
    else:
        print('Please provide the filepaths of the messages and categories '
              'datasets as the first and second argument respectively, as '
              'well as the filepath of the database to save the cleaned data '
              'to as the third argument. \n\nExample: python process_data.py '
              'disaster_messages.csv disaster_categories.csv '
              'DisasterResponse.db')


if __name__ == '__main__':
    main()
