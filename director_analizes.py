import pandas as pd
import country_analizes as ca

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
    ratings_df = pd.merge(ratings_df, name_df[['nconst', 'primaryName']], left_on=['directors'], right_on=['nconst'])
    ratings_df.drop('nconst', axis=1, inplace=True)
    ratings_df.rename(columns={'primaryName': 'directorName', 'averageRating': 'averRating'}, inplace=True)

    return ratings_df 

def analiza(n: int, start_date: int, end_date: int, genre: str):

    ratings_df, basics_df, crew_df, name_df = tsv_read(ratings_path, basics_path, crew_path, name_path)
    ratings_df = ca.filtring_by_date(start_date, end_date, basics_df, ratings_df)
    basics_df = filtring_by_genre(genre, basics_df)
    director_rating = preprocessing_task3(ratings_df, basics_df, crew_df, name_df)
    print(director_rating)



if __name__ == '__main__':
    analiza(n, start_date, end_date, genre)   