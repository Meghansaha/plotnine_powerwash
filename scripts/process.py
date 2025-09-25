# The data was originally found on https://osf.io/wpeh6/

# Data is in a zip file, need to extract
from zipfile import ZipFile

# Set the file name
data_path = "data/data.zip"

# Unzip it - create the zipfile object
test = ZipFile(data_path)

# Then extract it into the data folder
test.extractall()

# Pick up here with choosing data for the plot and exploring polars
