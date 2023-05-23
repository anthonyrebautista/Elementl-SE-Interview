from dagster import asset
from pandas import DataFrame, read_html, get_dummies, to_numeric
from sklearn.linear_model import LinearRegression as Regression

@asset
def country_stats() -> DataFrame:
    """ Raw country population data """
    df = read_html("https://tinyurl.com/mry64ebh", flavor='html5lib')[0]
    """ Creating labels for the columns """
    df.columns = ["country", "continent", "region", "pop_2022", "pop_2023", "pop_change"]
    """ Calculating the percentage change from 2022 to 2023 """
    df["pop_change"] = ((to_numeric(df["pop_2023"]) / to_numeric(df["pop_2022"])) - 1)*100
    return df

@asset
def change_model(country_stats: DataFrame) -> Regression:
    """ A regression model for each continent """
    """ Dropping any null values in the pop_change column """
    data = country_stats.dropna(subset=["pop_change"])
    """ Converting categorical data into dummy indicator variables """
    dummies = get_dummies(data[["continent"]])
    """ returning the linear regression model to see the impact of continent on the population change """
    return Regression().fit(dummies, data["pop_change"])

@asset
def continent_stats(country_stats: DataFrame, change_model: Regression) -> DataFrame:
    """ Summary of continent populations and change """
    df_groupby_continent = country_stats.groupby("continent").sum()
    """ inserts the slope of the best fitting line """
    df_groupby_continent["pop_change_factor"] = change_model.coef_
    return df_gropuby_continent
