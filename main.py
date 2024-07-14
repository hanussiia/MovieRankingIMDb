import argparse
import director_analizes as da
import country_analizes as ca
import os


def isExists(path):
    return os.path.exists(path)


def validate_pathes(args):
    pathes = [args.ratings_path, args.basics_path, args.crew_path, args.akas_path, args.name_path, args.gdp_path, args.population_path]
    for path in pathes:
        if not isExists(path):
            print(f"File {path} does not exist!")
            return False
    return True    
         

def main():
    parser = argparse.ArgumentParser(description='Analyze movie ratings by directors, by country or find which country has film hegemony')

    possible_genres = ['Documentary', 'Short', 'Animation', 'Comedy', 'Romance', 'Sport', 'News',
                        'Drama', 'Fantasy', 'Horror', 'Biography', 'Music', 'War', 'Crime', 'Western',
                        'Family', 'Adventure', 'Action', 'History', 'Mystery', 'Sci-Fi',
                        'Musical', 'Thriller', 'Film-Noir', 'Talk-Show', 'Game-Show', 'Reality-TV',
                        'Adult']
    
    parser.add_argument('analize',choices=['director', 'country'], help="Type of analyzes, by: 'country', 'director'")
    parser.add_argument('-start_date', default=2000, type=int, choices=range(1894, 2025), help='The start year for the analysis: in range(1894,2024). Default = 2000')
    parser.add_argument('-end_date', default=2020, type=int, required=False, choices=range(1894, 2025), help='The end year for the analysis: in range(1894,2024). Default = 2020')
    parser.add_argument('-n', default=10, type=int, required=False, help='The number of movies to be considered (default = 10)')

    parser.add_argument('-ratings_path', default='title.ratings.tsv.gz', type=str, help='Path to the title.ratings.tsv.gz TSV file')
    parser.add_argument('-basics_path', default='title.basics.tsv.gz', type=str, help='Path to the title.basics.tsv.gz TSV file')
    parser.add_argument('-crew_path', default='title.crew.tsv.gz', type=str, help='Path to the title.crew.tsv.gz TSV file')
    parser.add_argument('-akas_path', default='title.akas.tsv.gz', type=str, help='Path to the title.akas.tsv.gz TSV file')
    parser.add_argument('-name_path', default='name.basics.tsv.gz',type=str, help='Path to the name.basics.tsv.gz TSV file')

    parser.add_argument('gdp_path', type=str, help='Path to the GDP CSV file')
    parser.add_argument('population_path', type=str, help='Path to the Population CSV file')

    parser.add_argument('--genre', type=str, choices=possible_genres, help=f'The genre to filter movies')

    args = parser.parse_args()

    if args.start_date > args.end_date:
         raise Exception('Start_date must be lower then end_date or equals!')

    if validate_pathes(args):
        #try:
            if args.analize == 'country':
                    country_pathes = [args.ratings_path, args.basics_path, args.akas_path, args.gdp_path, args.population_path]
                    ca.analiza(country_pathes, args.n, args.start_date, args.end_date, args.genre)

            if args.analize == 'director':
                    director_pathes = [args.ratings_path, args.basics_path, args.crew_path, args.name_path]
                    da.analiza(director_pathes, args.n, args.start_date, args.end_date, args.genre)
        
        #except Exception as excep:
            # print(excep)
            # print(f"Something went wrong!")


if __name__ == '__main__':
    main()