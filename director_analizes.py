import pandas as pd
import country_analizes as ca
import numpy as np

start_date = 1970
end_date = 1980
genre = 'Crime'
n = 10

ratings_path = 'title.ratings.tsv.gz'
basics_path = 'title.basics.tsv.gz'
crew_path = 'title.crew.tsv.gz'
name_path = 'name.basics.tsv.gz'

def tsv_read(ratings_path:str, basics_path:str, crew_path:str, name_path: str):
    
    ratings_df = pd.read_csv(ratings_path, sep='\t')
    basics_df = pd.read_csv(basics_path, sep='\t')
    crew_df = pd.read_csv(crew_path, sep='\t')
    name_df = pd.read_csv(name_path, sep='\t')

    return ratings_df, basics_df, crew_df, name_df

def filtring_by_genre(genre: str, basics_df: pd.DataFrame):
    basics_df['genres'] = basics_df['genres'].astype(str)
    mask_genre = basics_df['genres'].apply(lambda x: genre in x.split(','))
    basics_df = basics_df[mask_genre]
    return basics_df

def preprocessing_task3(ratings_df: pd.DataFrame, basics_df: pd.DataFrame, crew_df: pd.DataFrame, name_df: pd.DataFrame):

    ratings_df = pd.merge(ratings_df, basics_df[['tconst', 'primaryTitle', 'isAdult', 'genres']], on='tconst', how='inner')
    ratings_df = pd.merge(ratings_df, crew_df[['tconst', 'directors']], on=['tconst'], how='inner')

    ratings_df = ratings_df.assign(directors=ratings_df['directors'].str.split(',')).explode('directors')
    ratings_df = pd.merge(ratings_df, name_df[['nconst', 'primaryName']], left_on=['directors'], right_on=['nconst'])

    ratings_df.drop('nconst', axis=1, inplace=True)
    ratings_df.rename(columns={'primaryName': 'directorName', 'averageRating': 'averRating'}, inplace=True)

    return ratings_df

def impact_directors_calc(df: pd.DataFrame):
    count_impact_df = df['directors'].value_counts().reset_index()

    total_sum_by_director = df.groupby('directors')['averRating'].sum().reset_index()
    total_sum_by_director.rename(columns={'averRating': 'sumRating'}, inplace=True)
    total_sum_by_director = pd.merge(total_sum_by_director, count_impact_df, on=['directors'], how='inner')
    total_sum_by_director['averRating'] = total_sum_by_director['sumRating'] / total_sum_by_director['count']
    
    total_sum_by_director = total_sum_by_director.sort_values('count', ascending=False)
    total_sum_by_director = total_sum_by_director.reset_index(drop=True)

    total_sum_by_director.drop('sumRating', axis=1, inplace=True)
    return total_sum_by_director

def stand_deriv(df: pd.DataFrame, impact_df: pd.DataFrame, column_group: str, column_for_std: str):
    aveRating_of_directors_array = df.groupby(column_group)[column_for_std].apply(list).reset_index()
    impact_df['std'] = aveRating_of_directors_array[column_for_std].apply(lambda x: np.std(np.array(x)))

    impact_df = impact_df.sort_values('std', ascending=False)
    impact_df = impact_df.reset_index(drop=True)
    
    return impact_df


def analiza(n: int, start_date: int, end_date: int, genre: str):

    ratings_df, basics_df, crew_df, name_df = tsv_read(ratings_path, basics_path, crew_path, name_path)
    ratings_df = ca.filtring_by_date(start_date, end_date, basics_df, ratings_df)
    basics_df = filtring_by_genre(genre, basics_df)
    director_rating = preprocessing_task3(ratings_df, basics_df, crew_df, name_df)

    impact_directors = impact_directors_calc(director_rating)    

    impact_directors = stand_deriv(director_rating, impact_directors, 'directors', 'averRating')
    print(impact_directors)

if __name__ == '__main__':
    analiza(n, start_date, end_date, genre)   