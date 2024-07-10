import pandas as pd
import country_index as ci

start_date = 1970
end_date = 1980
n = 10

def csv_gdp_ppl(gdp_path: str, population_path: str):
    gdp_df = pd.read_csv(gdp_path, sep=',')
    gdp_df.rename(columns={'Value': 'GDP'}, inplace=True)

    ppl_df = pd.read_csv(population_path, sep=',')
    ppl_df.rename(columns={'Value': 'Population'}, inplace=True)

    gdp_ppl_df = pd.merge(gdp_df, ppl_df, on=['Country Name', 'Country Code', 'Year'], how='inner')
    gdp_ppl_df.rename(columns={'Country Name': 'CountryName', 'Country Code': 'CountryCode'}, inplace=True)
    #gdp_ppl_df['Ratio GDP/POP'] = gdp_ppl_df['GDP'] / gdp_ppl_df['Population']
    #gdp_ppl_df['Country Name']= gdp_ppl_df['Country Code'].astype(str).map(ci.COUNTRY_INDEX)

    return gdp_ppl_df
 
def tsv_read(ratings_path:str, basics_path:str, akas_path:str):
    
    ratings_df = pd.read_csv(ratings_path, sep='\t')
    basics_df = pd.read_csv(basics_path, sep='\t')
    region_df = pd.read_csv(akas_path, sep='\t')

    return ratings_df, basics_df, region_df


def filtring_by_date(start_date:int, end_date: int, basics_df:pd.DataFrame, ratings_df:pd.DataFrame):

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

def sorting(df: pd.DataFrame, column1: str, column2: str):
    
    df['Total'] = (df[column1]*1.5 + df[column2]*0.5)/2

    df = df.sort_values('Total', ascending=False)
    df = df.reset_index(drop=True)

    return df


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

def task1_preprocessing(n, start_date, end_date):
    """
    data loading, initial filtering, sorting, df-creation.
    """
    ratings_df, basics_df, region_df = tsv_read('title.ratings.tsv.gz', 'title.basics.tsv.gz', 'title.akas.tsv.gz')
    ratings_df = filtring_by_date(start_date, end_date, basics_df, ratings_df)
    
    normalization_min_max(ratings_df, 'numVotes', 'numVotes')
    ratings_df = sorting(ratings_df, 'averageRating', 'numVotes')
    ratings_df = ratings_df.head(n)
    
    akas_filter_region(region_df)
        
    ratings_df = ratings_df.merge(region_df[['titleId', 'title', 'region']], left_on='tconst', right_on='titleId', how='left')
    ratings_df = cleaning_rating_data(ratings_df)

    

    return ratings_df



def task1_weak_impact_calculation(ratings_df:pd.DataFrame):

    impact = ratings_df['region'].value_counts().reset_index()
    impact.rename(columns={'count': 'weakImpact'}, inplace=True)

    total_sum_by_region = ratings_df.groupby('region')['Total'].sum().reset_index()
    total_sum_by_region.rename(columns={'Total': 'movieRatingSum'}, inplace=True)

    return impact, total_sum_by_region


def task1_postprocessing_and_display(res_rating):
    res_rating['country'] = res_rating['region'].astype(str).map(ci.COUNTRY_INDEX)
    cols_to_display = ['region', 'country', 'movieRatingSum', 'weakImpact', 'movieRatingTop', 'countRating', 'RatingQuality', 'averageMovieRating']

    print(res_rating[cols_to_display].head(10))
    return res_rating[cols_to_display]


def analiza(n: int, start_date: int, end_date: int):

    ratings_df= task1_preprocessing(n, start_date, end_date)
    impact, total_sum_by_region = task1_weak_impact_calculation(ratings_df)

    res_rating = total_sum_by_region.merge(impact, on=['region'], how='inner')
    normalization_min_max(res_rating, 'movieRatingTop', 'movieRatingSum')

    normalization_min_max(res_rating, 'countRating', 'weakImpact')
    res_rating = sorting(res_rating, 'movieRatingTop', 'countRating')
    res_rating.rename(columns={'Total': 'RatingQuality'}, inplace=True)

    res_rating['averageMovieRating'] = res_rating['movieRatingSum'] / res_rating['weakImpact']
    res_rating = res_rating.reset_index(drop=True)

    res_rating = task1_postprocessing_and_display(res_rating)

    

    #CINEMATIC IMPACT
    gdp_ppl_df = csv_gdp_ppl('gdp.csv', 'population.csv')
    gdp_ppl_df = gdp_ppl_df[gdp_ppl_df['Year'] == end_date]
    print(gdp_ppl_df)

    impact = impact.sort_values('weakImpact', ascending=False)
    impact = impact.reset_index(drop=True)

    cinematic_impact = pd.DataFrame({
        'Country': impact['region'],
        'WeakImpactRating': impact.index + 1
    }).reset_index(drop=True)

    # Create a dictionary mapping country names to their indices in table3
    #country_to_weakImpact = {country: idx for idx, country in enumerate(weakImpact['region'])}
    
    # Add the 'Ranking 2' column to table1
    #cinematic_impact['RatingWeakImpact'] = cinematic_impact['Country'].map(country_to_weakImpact) + 1

    normalization_min_max(gdp_ppl_df, 'GDP_normalized', 'GDP')
    normalization_min_max(gdp_ppl_df, 'Population_normalized', 'Population')

    weights = {
        'GDP': 0.2,
        'Population': 0.1,
        'MoviesCount': 0.3,
        'AverageRating': 0.4    
    }

    impact['strongImpact'] = (
        weights['GDP'] * gdp_ppl_df['GDP_normalized'] +
        weights['Population'] * gdp_ppl_df['Population_normalized'] +
        weights['MoviesCount'] * res_rating['countRating'] +
        weights['AverageRating'] * res_rating['averageMovieRating']
    )

    impact = impact.sort_values('strongImpact', ascending=False)
    impact = impact.reset_index(drop=True)

    country_to_strongImpact = {country: idx for idx, country in enumerate(impact['region'])}
    cinematic_impact['RatingStrongImpact'] = cinematic_impact['Country'].map(country_to_strongImpact) + 1

    gdp_ppl_df = gdp_ppl_df.sort_values('GDP', ascending=False)
    gdp_ppl_df = gdp_ppl_df.reset_index(drop=True)
    country_to_gpd = {country: idx for idx, country in enumerate(gdp_ppl_df['CountryName'])}

    gdp_ppl_df = gdp_ppl_df.sort_values('Population', ascending=False)
    gdp_ppl_df = gdp_ppl_df.reset_index(drop=True)
    country_to_pop = {country: idx for idx, country in enumerate(gdp_ppl_df['CountryName'])}
    
    cinematic_impact['Country'] = cinematic_impact['Country'].astype(str).map(ci.COUNTRY_INDEX)
    cinematic_impact['RatingGDP'] = cinematic_impact['Country'].map(country_to_gpd) + 1
    cinematic_impact['RatingPopulation'] = cinematic_impact['Country'].map(country_to_pop) + 1

    #cinematic_impact = pd.merge(impact, gdp_ppl_df, left_on=['country'], right_on=['Country Name'], how='left')
    #cinematic_impact = cinematic_impact.drop(['movieRatingSum', 'movieRatingTop', 'countRating', 'CountryCode'], axis=1, inplace=True)
    print(cinematic_impact)

analiza(n, start_date, end_date)