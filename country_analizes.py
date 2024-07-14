import pandas as pd
import country_index as ci
import numpy as np


def checking_columns_correctness(df: pd.DataFrame):
    columns = ['Country Name', 'Country Code', 'Year', 'Value']
    if not pd.Series(columns).isin(df.columns).all():
        print(f"Please check if GDP or Population tables contain columns: {columns}")   
        raise Exception('Wrong columns in GDP or Population DataFrame')    


def csv_gdp_ppl(gdp_path: str, population_path: str):
    gdp_df = pd.read_csv(gdp_path, sep=',')
    checking_columns_correctness(gdp_df)
    gdp_df.rename(columns={'Value': 'GDP'}, inplace=True)

    ppl_df = pd.read_csv(population_path, sep=',')
    checking_columns_correctness(ppl_df)
    ppl_df.rename(columns={'Value': 'Population'}, inplace=True)

    dataFrames_paths = {
        'ratings_df': gdp_path,
        'basics_df': population_path,
    }

    dataFrames = {
        'ratings_df': gdp_df,
        'basics_df': ppl_df,
    }

    for name, df in dataFrames.items():
        if df.empty:
            print(f'Data Frame {name} (path: {dataFrames_paths[name]}) is empty!')
            raise Exception(name)

    gdp_ppl_df = pd.merge(gdp_df, ppl_df, on=['Country Name', 'Country Code', 'Year'], how='inner')
    gdp_ppl_df.rename(columns={'Country Name': 'CountryName', 'Country Code': 'CountryCode'}, inplace=True)

    return gdp_ppl_df

 
def tsv_read(ratings_path:str, basics_path:str, akas_path:str):
    ratings_df = pd.read_csv(ratings_path, sep='\t')
    basics_df = pd.read_csv(basics_path, sep='\t')
    region_df = pd.read_csv(akas_path, sep='\t')

    return ratings_df, basics_df, region_df


def filtering_by_date(start_date:int, end_date: int, basics_df:pd.DataFrame, ratings_df:pd.DataFrame):
    basics_df.replace('\\N', 0, inplace=True)

    mask_basic = (basics_df['titleType'] == "movie") & (basics_df['startYear'].astype(int) >= start_date) & (basics_df['startYear'].astype(int) <= end_date)
    basics_df = basics_df[mask_basic]

    ratings_df = ratings_df[ratings_df['tconst'].isin(basics_df['tconst'])]
    ratings_df = ratings_df.merge(basics_df[['tconst', 'startYear', 'originalTitle']], on='tconst', how='left')
    ratings_df = ratings_df.reset_index(drop=True)
    
    return ratings_df


def normalization_min_max(df:pd.DataFrame, column_result: str, column_for_norm: str):
    df[column_result] = (10 * (df[column_for_norm] - df[column_for_norm].min()) \
                              /(df[column_for_norm].max() - df[column_for_norm].min()))


def akas_filter_region(region_df):
    mask_region1 = region_df['isOriginalTitle'] == 1
    region_df_origin = region_df[mask_region1]

    mask_region2 = region_df['isOriginalTitle'] == 0
    region_df_zero = region_df[mask_region2]

    region_df = region_df_zero.merge(region_df_origin[['titleId', 'title']], on=['titleId', 'title'], how='inner')
    

def cleaning_rating_data(ratings_df: pd.DataFrame):
    ratings_df.drop('titleId', axis=1, inplace=True)
    ratings_df.drop('originalTitle', axis=1, inplace=True)
    ratings_df.drop(ratings_df[ratings_df['region'] == '\\N'].index, inplace=True)
    ratings_df = ratings_df.drop_duplicates(subset=['tconst', 'title', 'region'])
    return ratings_df


def filtring_by_genre(genre: str, basics_df: pd.DataFrame):

    basics_df['genres'] = basics_df['genres'].astype(str)
    mask_genre = basics_df['genres'].apply(lambda x: genre in x.split(','))
    basics_df = basics_df[mask_genre]
    return basics_df


def final_rating(df: pd.DataFrame, column1: str, column2: str):
    df['Total'] = (df[column1]*1.5 + df[column2]*0.5)/2

    df = df.sort_values('Total', ascending=False)
    df = df.reset_index(drop=True)

    return df


def task1_preprocessing(n, start_date, end_date, pathes, genre:str):
    """
    data loading, initial filtering, sorting, df-creation.
    """
    ratings_path, basics_path, akas_path = pathes
    ratings_df, basics_df, region_df = tsv_read(*pathes)

    if genre != None:
        print(f"Rating for {genre}")
        basics_df = filtring_by_genre(genre, basics_df)

    ratings_df = filtering_by_date(start_date, end_date, basics_df, ratings_df)

    dataFrames_paths = {
        'ratings_df': ratings_path,
        'basics_df': basics_path,
        'region_df': akas_path
    }

    dataFrames = {
        'ratings_df': ratings_df,
        'basics_df': basics_df,
        'region_df': region_df,
    }

    for name, df in dataFrames.items():
        if df.empty:
            print(f'Data Frame {name} (path: {dataFrames_paths[name]}) is empty!')
            raise Exception(name)
        
    normalization_min_max(ratings_df, 'numVotes', 'numVotes')
    ratings_df = final_rating(ratings_df, 'averageRating', 'numVotes')
    ratings_df = ratings_df.head(n)
    
    akas_filter_region(region_df)
        
    ratings_df = ratings_df.merge(region_df[['titleId', 'title', 'region']], left_on='tconst', right_on='titleId', how='left')
    ratings_df = cleaning_rating_data(ratings_df)

    
    return ratings_df


def task1_impact_calculation(ratings_df:pd.DataFrame):
    impact = ratings_df['region'].value_counts().reset_index()
    impact.rename(columns={'count': 'weakImpact'}, inplace=True)

    total_sum_by_region = ratings_df.groupby('region')['Total'].sum().reset_index()
    total_sum_by_region.rename(columns={'Total': 'movieRatingSum'}, inplace=True)

    return impact, total_sum_by_region


def task1_2_postprocessing_and_display(res_rating):
    res_rating['country'] = res_rating['region'].astype(str).map(ci.COUNTRY_INDEX)
    cols_to_display = ['region', 'country', 'movieRatingSum', 'weakImpact', 'averageMovieRating', 'movieRatingTop', 'countRating', 'RatingQuality',]
    res_rating = res_rating.sort_values('RatingQuality', ascending=False).reset_index()
    print(res_rating[cols_to_display].head(10))
    return res_rating[cols_to_display]


def sorting_and_mapping(df: pd.DataFrame, res_df: pd.DataFrame, res_column_name: str, country_column_res: str, country_column: str, column_for_sorting: str):
    df = df.sort_values(column_for_sorting, ascending=False)
    df = df.reset_index(drop=True)
    country_to_map = {country: idx for idx, country in enumerate(df[country_column])}
    res_df[res_column_name] = res_df[country_column_res].map(country_to_map) + 1

    return res_df

def analiza(pathes, n: int, start_date: int, end_date: int, genre: str=None):
    ratings_path, basics_path, akas_path, gdp_path, population_path = pathes

    ratings_df= task1_preprocessing(n, start_date, end_date, (ratings_path, basics_path, akas_path), genre)
    impact, total_sum_by_region = task1_impact_calculation(ratings_df)

    res_rating = total_sum_by_region.merge(impact, on=['region'], how='inner')

    res_rating['averageMovieRating'] = res_rating['movieRatingSum'] / res_rating['weakImpact']

    normalization_min_max(res_rating, 'movieRatingTop', 'averageMovieRating')
    normalization_min_max(res_rating, 'countRating', 'weakImpact')

    res_rating = final_rating(res_rating, 'movieRatingTop', 'countRating')
    res_rating.rename(columns={'Total': 'RatingQuality'}, inplace=True)

    res_rating = task1_2_postprocessing_and_display(res_rating)


    #CINEMATIC IMPACT
    gdp_ppl_df = csv_gdp_ppl(gdp_path, population_path)
    max_year_gdp = gdp_ppl_df['Year'].max()
    gdp_ppl_df = gdp_ppl_df[gdp_ppl_df['Year'] == min(end_date, max_year_gdp)]

    impact = impact.sort_values('weakImpact', ascending=False)
    impact = impact.reset_index(drop=True)

    cinematic_impact = pd.DataFrame({
        'Country': impact['region'],
        'WeakImpactRating': impact.index + 1
    }).reset_index(drop=True)

    normalization_min_max(gdp_ppl_df, 'GDP_normalized', 'GDP')
    normalization_min_max(gdp_ppl_df, 'Population_normalized', 'Population')

    weights = {
        'GDP': 0.1,
        'Population': 0.05,
        'MoviesCount': 0.35,
        'AverageRating': 0.5    
    }

    cinematic_impact['Region'] = cinematic_impact['Country']
    cinematic_impact = pd.merge(cinematic_impact, res_rating[['region', 'movieRatingTop', 'countRating']], left_on='Region', right_on='region', how='inner')
    cinematic_impact.drop('region', axis=1, inplace=True)

    cinematic_impact['Country'] = cinematic_impact['Country'].astype(str).map(ci.COUNTRY_INDEX)
    cinematic_impact = pd.merge(cinematic_impact, gdp_ppl_df[['CountryName', 'GDP_normalized', 'Population_normalized']], left_on='Country', right_on='CountryName', how='inner')
    cinematic_impact.drop('CountryName', axis=1, inplace=True)


    cinematic_impact['strongImpact'] = (
        weights['GDP'] * cinematic_impact['GDP_normalized'] +
        weights['Population'] * cinematic_impact['Population_normalized'] +
        weights['MoviesCount'] * cinematic_impact['countRating'] +
        weights['AverageRating'] * cinematic_impact['movieRatingTop']
    )
    
    cinematic_impact = cinematic_impact.sort_values('strongImpact', ascending=False)
    cinematic_impact = cinematic_impact.reset_index(drop=True)
    cinematic_impact['RatingStrongImpact'] = cinematic_impact.index + 1
    #cinematic_impact = sorting_and_mapping(cinematic_impact, cinematic_impact, 'RatingStrongImpact', 'Region', 'Region', 'strongImpact')
    
    cinematic_impact = sorting_and_mapping(gdp_ppl_df, cinematic_impact, 'RatingGDP', 'Country', 'CountryName', 'GDP')
    cinematic_impact = sorting_and_mapping(gdp_ppl_df, cinematic_impact, 'RatingPopulation', 'Country', 'CountryName', 'Population')

    # condition_to_drop = (pd.isna(cinematic_impact['RatingGDP'])) | (pd.isna(cinematic_impact['RatingPopulation']))
    # cinematic_impact.drop(cinematic_impact[condition_to_drop], axis=1, inplace=True)

    cinematic_impact = cinematic_impact.sort_values('RatingStrongImpact', ascending=True)
    cinematic_impact = cinematic_impact.reset_index(drop=True)
    print(cinematic_impact.head(10))
