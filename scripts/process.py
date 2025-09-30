# The data was originally found on https://osf.io/wpeh6/

#%%  Module Load-in--------------------------------------------------------
#==========================================================================#
from zipfile import ZipFile 
from requests import get
from io import BytesIO
import polars as pl

#==========================================================================#
#%% Data Fetching----------------------------------------------------------
url_zip = "https://osf.io/download/j48qf/"
response = get(url_zip)
lst_files = (
    "data/demographics.csv",
    "data/mood_reported.csv",
    "data/update_current_state.csv"
    )

# My first function in Python!!
# Will extract the document name from the file path
def grab_file_name(string):
    cleaned_string = string.removeprefix("data/").removesuffix(".csv")
    return cleaned_string

# Create a dictionary to store dataframes
dataframes = {}

# Iterate through to 
with ZipFile(BytesIO(response.content)) as file_zip:
    for file in lst_files:
        clean_name = grab_file_name(file)
        with file_zip.open(file) as csv_file:
            dataframes[clean_name] = pl.read_csv(csv_file)

#==========================================================================#
#%% Data Wrangling----------------------------------------------------------
# First, get ages of players
df_players = (
    dataframes["demographics"]
    .select("pid", "age")
)

# Then get level progression and reported moods
df_level_moods = (
    dataframes["mood_reported"]
    .select("pid", "LevelProgressionAmount", "response")
)

# Join the dfs to prep for grouping work
df_demo_moods_init = (
    df_level_moods.join(
    df_players,
    on = "pid",
    how = "left")
)

# Create and order of age groups:
age_order = {
    "18-24 Years Old": 1,
    "25-34 Years Old": 2,
    "35-44 Years Old": 3,
    "45-54 Years Old": 4,
    "55-64 Years Old": 5,
    "65+ Years Old": 6,
    "NULL": 7,
}

# Create the age groups and perform group calculations
df_demo_moods = (
    df_demo_moods_init
    .filter(
        pl.col("LevelProgressionAmount").is_not_null() & 
        pl.col("response").is_not_null() &
        pl.col("age").is_not_null()
    )
    .with_columns(
        pl.when((pl.col("age") >= 18) & (pl.col("age") <= 25))
        .then(pl.lit("18-24 Years Old"))
        .when((pl.col("age") >= 25) & (pl.col("age") <= 34))
        .then(pl.lit("25-34 Years Old"))
        .when((pl.col("age") >= 35) & (pl.col("age") <= 44))
        .then(pl.lit("35-44 Years Old"))
        .when((pl.col("age") >= 45) & (pl.col("age") <= 54))
        .then(pl.lit("45-54 Years Old"))
        .when((pl.col("age") >= 55) & (pl.col("age") <= 64))
        .then(pl.lit("55-64 Years Old"))
        .when((pl.col("age") >= 65))
        .then(pl.lit("65+ Years Old"))
        .otherwise(pl.lit("NULL"))
        .alias("age_group")
    )
    .with_columns(
        pl.when(pl.col("age_group") == "18-24 Years Old").then(1)
        .when(pl.col("age_group") == "25-34 Years Old").then(2)
        .when(pl.col("age_group") == "35-44 Years Old").then(3)
        .when(pl.col("age_group") == "45-54 Years Old").then(4)
        .when(pl.col("age_group") == "55-64 Years Old").then(5)
        .when(pl.col("age_group") == "65+ Years Old").then(6)
        .otherwise(7)
        .alias("age_group_order")
    )
    .group_by(["age_group", "age_group_order"])
    .agg([
    pl.len().alias("n"),
    pl.mean("response").alias("avg_mood"),
    pl.mean("LevelProgressionAmount").alias("avg_prog")
    ])
    .sort("age_group_order")
)








# Q1: Progression amount -vs- well being score by age