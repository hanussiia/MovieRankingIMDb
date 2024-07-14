import pandas as pd
import country_analizes as ca
import numpy as np

def tsv_read(ratings_path:str, basics_path:str, crew_path:str, name_path: str):
    
    ratings_df = pd.read_csv(ratings_path, sep='\t')
    basics_df = pd.read_csv(basics_path, sep='\t')
    crew_df = pd.read_csv(crew_path, sep='\t')
    name_df = pd.read_csv(name_path, sep='\t')

    dataFrames_paths = {
        'ratings_df': ratings_path,
        'basics_df': basics_path,
        'crew_df': crew_path,
        'name_df': name_path
    }

    dataFrames = {
        'ratings_df': ratings_df,
        'basics_df': basics_df,
        'crew_df': crew_df,
        'name_df': name_df
    }

    for name, df in dataFrames.items():
        if df.empty:
            print(f'Data Frame {name} (path: {dataFrames_paths[name]}) is empty!')
            raise Exception(name)
        
    return ratings_df, basics_df, crew_df, name_df


def preprocessing_task3(ratings_df: pd.DataFrame, basics_df: pd.DataFrame, crew_df: pd.DataFrame, name_df: pd.DataFrame):

    ratings_df = pd.merge(ratings_df, basics_df[['tconst', 'primaryTitle', 'genres']], on='tconst', how='inner')
    ratings_df = pd.merge(ratings_df, crew_df[['tconst', 'directors']], on=['tconst'], how='inner')

    ratings_df = ratings_df.assign(directors=ratings_df['directors'].str.split(',')).explode('directors')
    ratings_df = pd.merge(ratings_df, name_df[['nconst', 'primaryName']], left_on=['directors'], right_on=['nconst'])

    ratings_df.drop('nconst', axis=1, inplace=True)
    ratings_df.rename(columns={'primaryName': 'directorName', 'averageRating': 'averRating'}, inplace=True)

    return ratings_df


def impact_directors_calc(df: pd.DataFrame):
    count_impact_df = df['directors'].value_counts().reset_index()
    total_sum_by_director = df.groupby(['directors', 'directorName'])['averRating'].sum().reset_index()
    total_numVotes_by_director = df.groupby(['directors'])['numVotes'].sum().reset_index()
    total_sum_by_director.rename(columns={'averRating': 'sumRating'}, inplace=True)
    
    total_sum_by_director = pd.merge(total_sum_by_director, count_impact_df, on=['directors'], how='inner')
    total_sum_by_director = pd.merge(total_sum_by_director, total_numVotes_by_director, on=['directors'], how='inner')
    total_sum_by_director['averRating'] = total_sum_by_director['sumRating'] / total_sum_by_director['count']
    total_sum_by_director['averNumVotes'] = total_sum_by_director['numVotes'] / total_sum_by_director['count']
    
    total_sum_by_director = total_sum_by_director.sort_values('count', ascending=False)
    total_sum_by_director = total_sum_by_director.reset_index(drop=True)

    total_sum_by_director.drop('sumRating', axis=1, inplace=True)
    total_sum_by_director.drop('numVotes', axis=1, inplace=True)

    return total_sum_by_director


def stand_deriv(df: pd.DataFrame, impact_df: pd.DataFrame, column_group: str, column_for_std: str):
    aveRating_of_directors_array = df.groupby(column_group)[column_for_std].apply(list).reset_index()
    impact_df['std'] = aveRating_of_directors_array[column_for_std].apply(lambda x: np.std(np.array(x)))

    impact_df = impact_df.sort_values('std', ascending=False)
    impact_df = impact_df.reset_index(drop=True)
    
    return impact_df


def final_rating(df: pd.DataFrame):
    # global_rating = int(df['averRating'].mean())
    # global_votes= int(df['averNumVotes'].mean())
   
    ca.normalization_min_max(df, 'count_norm', 'count')
    ca.normalization_min_max(df, 'averRating_norm', 'averRating')
    ca.normalization_min_max(df, 'averVotes_norm', 'averNumVotes')

    weights = {
        'rating': 0.4,
        'votes': 0.4,
        'count': 0.2
    }

    df['FinalRating'] = df['count_norm'] * weights['count'] + weights['rating'] * df['averRating_norm'] + weights['votes'] * df['averVotes_norm']
   
    df = df.sort_values('FinalRating', ascending=False)
    df = df.reset_index(drop=True)
    return df

def display(impact_directors):
    cols_to_display = ['directors', 'directorName', 'count', 'averRating', 'averNumVotes', 'FinalRating']
    impact_directors = impact_directors.round({"averRating":2, "FinalRating":2}) 
    
    impact_directors = impact_directors[cols_to_display]
    impact_directors = impact_directors.sort_values('FinalRating', ascending=False)
    impact_directors = impact_directors.reset_index(drop=True)
    
    return impact_directors[cols_to_display]

def analiza(pathes, n: int, start_date: int, end_date: int, genre:str=None):
        
    ratings_path, basics_path, crew_path, name_path = pathes
    ratings_df, basics_df, crew_df, name_df = tsv_read(ratings_path, basics_path, crew_path, name_path)

    basics_df = basics_df[(basics_df['startYear'] != '\\N') & (basics_df['titleType'] == 'movie')]

    if genre != None:
        print(f"Rating for {genre}")
        basics_df = ca.filtring_by_genre(genre, basics_df)
    director_rating = preprocessing_task3(ratings_df, basics_df, crew_df, name_df)

    impact_directors = impact_directors_calc(director_rating)    

    # impact_directors = stand_deriv(director_rating, impact_directors, 'directors', 'averRating')
    # impact_directors.rename(columns={'std': 'stdRating'}, inplace=True)

    impact_directors = final_rating(impact_directors)
    impact_directors = display(impact_directors)
    print('--------------------')
    print("RATING BY DIRECTORS")
    print('--------------------')
    print(impact_directors.head(10))