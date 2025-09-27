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
# Players to be included in the visual
df_players = (
    dataframes["demographics"]
    .filter(pl.col("responses").is_not_null())
    .select("pid", "age")
)



# Q1: Progression amount -vs- well being score by age