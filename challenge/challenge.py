####
#Imports
####
import json
import pandas as pd
import numpy as np
import re
import sys
#!{sys.executable} -m pip install psycopg2-binary
from sqlalchemy import create_engine
from config import db_password
import time

####
#Global
####
file_dir = '/Users/pbryzek/Desktop/CodeBases/data/movies-etl/module/res/'

####
#Functions
####
def movies_plots(movies_df):
    movies_df.fillna(0).plot(x='running_time', y='runtime', kind='scatter')
    movies_df.fillna(0).plot(x='budget_wiki',y='budget_kaggle', kind='scatter')
    movies_df.fillna(0).plot(x='box_office', y='revenue', kind='scatter')
    movies_df.fillna(0)[movies_df['box_office'] < 10**9].plot(x='box_office', y='revenue', kind='scatter')
    movies_df[['release_date_wiki','release_date_kaggle']].plot(x='release_date_wiki', y='release_date_kaggle', style='.')

    return movies_df

def rename_cols(movies_df):
    movies_df = movies_df.loc[:, ['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
    'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count', 'genres','original_language','overview','spoken_languages','Country',
    'production_companies','production_countries','Distributor','Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
    ]]
    movies_df.rename({
        'id':'kaggle_id',
        'title_kaggle':'title',
        'url':'wikipedia_url',
        'budget_kaggle':'budget',
        'release_date_kaggle':'release_date',
        'Country':'country',
        'Distributor':'distributor',
        'Producer(s)':'producers',
        'Director':'director',
        'Starring':'starring',
        'Cinematography':'cinematography',
        'Editor(s)':'editors',
        'Writer(s)':'writers',
        'Composer(s)':'composers',
        'Based on':'based_on'
        }, axis='columns', inplace=True)
    return movies_df

def populate_ratings(ratings):
    ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')
    ratings['rating'].plot(kind='hist')
    ratings['rating'].describe()
    return ratings

def populate_kaggle(kaggle_metadata):
    #Kaggle
    kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')
    kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'
    kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
    kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
    #Assumption is that kaggle popularity is not numberic.
    kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
    kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])
    return kaggle_metadata

#Function to parse out the release_date info and add to a new column.
def add_release_date(wiki_movies_df):
    # Release Date
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    date_form_two = r'\d{4}.[01]\d.[123]\d'
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    date_form_four = r'\d{4}'
    
    wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
    
    return wiki_movies_df

#Function to parse out the budget info and add to a new column.
def add_budget(wiki_movies_df, form_one, form_two):
    #Budget
    budget = wiki_movies_df['Budget'].dropna()
    budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)

    matches_form_one = budget.str.contains(form_one, flags=re.IGNORECASE)
    matches_form_two = budget.str.contains(form_two, flags=re.IGNORECASE)
    budget = budget.str.replace(r'\[\d+\]\s*', '')

    wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    wiki_movies_df.drop('Budget', axis=1, inplace=True)

    return wiki_movies_df

#Function to parse out the box_office info and add to a new column.
def add_box_office(wiki_movies_df, form_one, form_two):
    #From the wiki_movies, drop all entries with empty values.
    box_office = wiki_movies_df['Box office'].dropna() 
    #Lambda function to check if each element in box_office is not a string
    box_office = box_office[box_office.map(lambda x: type(x) != str)]
    #Join the list and add a space in between each index, returned as a string.
    box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)
    
    #Assumption on all RegExs
    box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    wiki_movies_df.drop('Box office', axis=1, inplace=True)

    return wiki_movies_df

#Function to fill in missing data for a column pair then drops the redundant column.
def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
    df[kaggle_column] = df.apply(
        lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column]
        , axis=1)
    df.drop(columns=wiki_column, inplace=True)
    return df

def parse_dollars(s):
    # if s is not a string, return NaN
    if type(s) != str:
        return np.nan

    # if input is of the form $###.# million
    if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):
        # remove dollar sign and " million"
        s = re.sub('\$|\s|[a-zA-Z]','', s)
        # convert to float and multiply by a million
        value = float(s) * 10**6
        # return value
        return value
    # if input is of the form $###.# billion
    elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):
        # remove dollar sign and " billion"
        s = re.sub('\$|\s|[a-zA-Z]','', s)
        # convert to float and multiply by a billion
        value = float(s) * 10**9
        # return value
        return value
    # if input is of the form $###,###,###
    elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):
        # remove dollar sign and commas
        s = re.sub('\$|,','', s)
        # convert to float
        value = float(s)
        # return value
        return value
    # otherwise, return NaN
    else:
        return np.nan

#Function to combine alt titles into a list and merge the column names.
def clean_movie(movie):
    movie = dict(movie) #create a non-destructive copy
    alt_titles = {}
    # combine alternate titles into one list
    for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                'Hangul','Hebrew','Hepburn','Japanese','Literally',
                'Mandarin','McCune-Reischauer','Original title','Polish',
                'Revised Romanization','Romanized','Russian',
                'Simplified','Traditional','Yiddish']:
        if key in movie:
            alt_titles[key] = movie[key]
            movie.pop(key)
    if len(alt_titles) > 0:
        movie['alt_titles'] = alt_titles

    # merge column names
    def change_column_name(old_name, new_name):
        if old_name in movie:
            movie[new_name] = movie.pop(old_name)
    
    change_column_name('Adaptation by', 'Writer(s)')
    change_column_name('Country of origin', 'Country')
    change_column_name('Directed by', 'Director')
    change_column_name('Distributed by', 'Distributor')
    change_column_name('Edited by', 'Editor(s)')
    change_column_name('Length', 'Running time')
    change_column_name('Original release', 'Release date')
    change_column_name('Music by', 'Composer(s)')
    change_column_name('Produced by', 'Producer(s)')
    change_column_name('Producer', 'Producer(s)')
    change_column_name('Productioncompanies ', 'Production company(s)')
    change_column_name('Productioncompany ', 'Production company(s)')
    change_column_name('Released', 'Release Date')
    change_column_name('Release Date', 'Release date')
    change_column_name('Screen story by', 'Writer(s)')
    change_column_name('Screenplay by', 'Writer(s)')
    change_column_name('Story by', 'Writer(s)')
    change_column_name('Theme music composer', 'Composer(s)')
    change_column_name('Written by', 'Writer(s)')

    return movie

#Function to parse out the box_office info and add to a new column.
def add_running_time(wiki_movies_df):
    running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
    wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
    wiki_movies_df.drop('Running time', axis=1, inplace=True)
    return wiki_movies_df

#Function that connects to postrgres DB and 
def upload_data_sql(movies_df):
    db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"
    engine = create_engine(db_string)
    #Transfer the movies dataframe into a SQL table, replacing if exists.
    movies_df.to_sql(name='movies', con=engine,if_exists='replace')

    rows_imported = 0
    # get the start_time from time.time()
    start_time = time.time()
    #Read the ratings file in chunks
    for data in pd.read_csv(f'{file_dir}ratings.csv', chunksize=1000000):
        print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
        #Transfer the ratings data to SQL replacing if already existing.
        data.to_sql(name='ratings', con=engine, if_exists='replace')
        rows_imported += len(data)

        # add elapsed time to final print out
        print(f'Done. {time.time() - start_time} total seconds elapsed')
        pass

#Challenge function entry point as defined by the challenge.
def challenge(wiki_movies_raw, kaggle_metadata, ratings):
    # Add try except blocks where appropriate to make it run automatically
    # Document 5 assumptions

    #Loop through and call clean_movie on each movie in the JSON
    clean_movies = [clean_movie(movie) for movie in wiki_movies_raw]
    #Convert the movies JSON into a dataframe
    wiki_movies_df = pd.DataFrame(clean_movies)
    #Sort the dataframe on the columns
    sorted(wiki_movies_df.columns.tolist())
    
    #Assumption 1: Assume that all imdb_links contain a tt and that all IDs are of length 7. What if another string as tt1234567 in the url
    try:
        #Extract with RegEx the imdb_link from URL imdb_link e.g. (https://www.imdb.com/title/tt1234567/)
        wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
        #Drop all duplicates of the same imdb_id to get unique entries.
        wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)
    except Exception as e:
        print("Exception caught when parsing imdb_id: ", e)
    
    #Iterate across all columns in the dataframe, if the column is null, sum the entire column's values. If it is less than 90% of the number of null values, then keep the column,
    wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
    #Based off the columns to keep, filter to a new dataframe.
    wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]

    #RegEx, common for Budget and box_office
    form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illi?on)'
    #Add Box Office column
    wiki_movies_df = add_box_office(wiki_movies_df, form_one, form_two)
    #Add Budget column
    wiki_movies_df = add_budget(wiki_movies_df, form_one, form_two)
    #Add Release Date column
    wiki_movies_df = add_release_date(wiki_movies_df)
    #Add Running Time Column
    wiki_movies_df = add_running_time(wiki_movies_df)
    #Populate Kaggle metadata
    kaggle_metadata = populate_kaggle(kaggle_metadata)

    #Ratings
    ratings = populate_ratings(ratings)

    #Merge movies and kaggle on the common imdb_id
    movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])

    # Competing data:
    # Wiki                     Movielens                Resolution
    #--------------------------------------------------------------------------
    # title_wiki               title_kaggle
    # running_time             runtime
    # budget_wiki              budget_kaggle
    # box_office               revenue
    # release_date_wiki        release_date_kaggle
    # Language                 original_language
    # Production company(s)    production_companies     

    movies_df = movies_plots(movies_df)
    movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')].index)

    movies_df['Language'].apply(lambda x: tuple(x) if type(x) == list else x).value_counts(dropna=False)
    movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)

    movies_df = fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
    movies_df = fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
    movies_df = fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')
    #Rename and reorder the columns
    movies_df = rename_cols(movies_df)

    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count()
    rating_counts = rating_counts.rename({'userId':'count'}, axis=1) 
    rating_counts = rating_counts.pivot(index='movieId',columns='rating', values='count')
    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]

    movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)

    upload_data_sql(movies_df)

def main():
    #Read data from Files 
    with open(f'{file_dir}wikipedia.movies.json', mode='r') as file:
        wiki_movies_raw = json.load(file)
    kaggle_metadata = pd.read_csv(f'{file_dir}movies_metadata.csv',low_memory=False)
    ratings = pd.read_csv(f'{file_dir}ratings.csv')

    #Call the main challenge function to run the analysis
    challenge(wiki_movies_raw, kaggle_metadata, ratings)

if __name__ == "__main__":
    main()