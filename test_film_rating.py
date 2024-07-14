import country_analizes as ca
import director_analizes as da
import country_index as ci
import pandas as pd 

def test_min_max():
    df = pd.DataFrame({'a':[1, 2, 3, 4, 5]})
    ca.normalization_min_max(df, 'b', 'a')
    assert 'b' in df.columns
    assert df['b'].min() == 0
    assert df['b'].max() == 10


def test_filtering_by_genre():
    df = pd.DataFrame({'a':[1,2,3], 'genres':['a,b', 'b,a', 'c,d']})
    df = ca.filtring_by_genre('a', df)
    assert len(df) == 2
