import pandas as pd
start_date = 1900
end_date = 1950
n = 10
 
def tsv_read(ratings_path:str, basics_path:str, akas_path:str):
    
    ratings_df = pd.read_csv(ratings_path, sep='\t', nrows=100000)
    basics_df = pd.read_csv(basics_path, sep='\t', nrows=100000)
    region_df = pd.read_csv(akas_path, sep='\t', nrows=100000)
    return ratings_df, basics_df, region_df


def filtring_by_date(start_date:int, basics_df:pd.DataFrame, ratings_df:pd.DataFrame):

    basics_df.replace('\\N', 0, inplace=True)

    mask_basic = (basics_df['titleType'] == "movie") & (basics_df['startYear'].astype(int) >= start_date) & (basics_df['startYear'].astype(int) <= end_date)
    basics_df = basics_df[mask_basic]

    ratings_df = ratings_df[ratings_df['tconst'].isin(basics_df['tconst'])]
    ratings_df = ratings_df.merge(basics_df[['tconst', 'startYear', 'originalTitle']], on='tconst', how='left')
    ratings_df = ratings_df.reset_index(drop=True)
    
    return ratings_df


def normalization_and_sorting(ratings_df:pd.DataFrame):
    #Normalizacja danych, gdzie numVotes ma ocenę od 1 do 10
    ratings_df['numVotes'] = (10*(ratings_df['numVotes']-ratings_df['numVotes'].min()) \
                              /(ratings_df['numVotes'].max()-ratings_df['numVotes'].min()))
    #normalizacja danych kolumny z oceną ogólną od 1 do 10
    ratings_df['Total'] = (ratings_df['averageRating'] + ratings_df['numVotes'])/2

    ratings_df = ratings_df.sort_values('Total', ascending=False)
    ratings_df = ratings_df.reset_index(drop=True)

    ratings_df = ratings_df.head(n)
    return ratings_df


def akas_filter_region(region_df):

    mask_region1 = region_df['isOriginalTitle'] == 1
    region_df_origin = region_df[mask_region1]

    mask_region2 = region_df['isOriginalTitle'] == 0
    region_df_zero = region_df[mask_region2]

    region_df = region_df_zero.merge(region_df_origin[['titleId', 'title']], on=['titleId', 'title'], how='inner')
    




if __name__ == '__main__':
    ratings_df, basics_df, region_df = tsv_read('title.ratings.tsv.gz', 'title.basics.tsv.gz', 'title.akas.tsv.gz')
    ratings_df = filtring_by_date(start_date, basics_df, ratings_df)
    ratings_df = normalization_and_sorting(ratings_df)
    #print(ratings_df)
    akas_filter_region(region_df)

    ratings_df = ratings_df.merge(region_df[['titleId', 'title', 'region']], left_on='tconst', right_on='titleId', how='left')
    ratings_df.drop('titleId', axis=1, inplace=True)
    ratings_df.drop('originalTitle', axis=1, inplace=True)
    ratings_df.drop(ratings_df[ratings_df['region'] == '\\N'].index, inplace=True)
    ratings_df = ratings_df.drop_duplicates(subset=['tconst', 'title', 'region'])
    print(ratings_df)

    sum_by_region = ratings_df['region'].value_counts().reset_index()
    sum_by_rating = ratings_df.groupby('region')['Total'].sum().reset_index()
    sum_by_rating.rename(columns={'Total': 'movieRatingSum'}, inplace=True)
    print(sum_by_rating)
    print(sum_by_region)

    sum_by_rating = sum_by_rating.merge(sum_by_region, on=['region'], how='inner')
    sum_by_rating['movieRating %'] = (10*(sum_by_rating['movieRatingSum']-sum_by_rating['movieRatingSum'].min()) \
                              /(sum_by_rating['movieRatingSum'].max()-sum_by_rating['movieRatingSum'].min()))
    
    sum_by_rating['count %'] = (10*(sum_by_rating['count']-sum_by_rating['count'].min()) \
                              /(sum_by_rating['count'].max()-sum_by_rating['count'].min()))
    
    sum_by_rating['movieMakerRating %'] = (sum_by_rating['movieRating %'] + sum_by_rating['count %'])/20
    sum_by_rating['averageMovieRating'] = sum_by_rating['movieRatingSum']/sum_by_rating['count']
    sum_by_rating = sum_by_rating.sort_values('movieMakerRating %', ascending=False)
    sum_by_rating = sum_by_rating.reset_index(drop=True)

    sum_by_rating = sum_by_rating.head(10)
    print(sum_by_rating)