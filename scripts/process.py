# The data was originally found on https://osf.io/wpeh6/

#%%  Module Load-in--------------------------------------------------------
#==========================================================================#
from zipfile import ZipFile 
from requests import get
from io import BytesIO
import plotnine as pn
import polars as pl
import matplotlib.font_manager as fm
import os

#==========================================================================#
#%% Data Fetching----------------------------------------------------------
url_zip = "https://osf.io/download/j48qf/"
response = get(url_zip)
lst_files = (
    "data/demographics.csv",
    "data/mood_reported.csv"
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
    how = "left"
    )
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
        .then(pl.lit("18-24\nYears Old"))
        .when((pl.col("age") >= 25) & (pl.col("age") <= 34))
        .then(pl.lit("25-34\nYears Old"))
        .when((pl.col("age") >= 35) & (pl.col("age") <= 44))
        .then(pl.lit("35-44\nYears Old"))
        .when((pl.col("age") >= 45) & (pl.col("age") <= 54))
        .then(pl.lit("45-54\nYears Old"))
        .when((pl.col("age") >= 55) & (pl.col("age") <= 64))
        .then(pl.lit("55-64\nYears Old"))
        .when((pl.col("age") >= 65))
        .then(pl.lit("65+\nYears Old"))
        .otherwise(pl.lit("NULL"))
        .alias("age_group")
    )
    .with_columns(
        # This helps to sort the group for the plot
        pl.when(pl.col("age_group") == "18-24\nYears Old").then(1)
        .when(pl.col("age_group") == "25-34\nYears Old").then(2)
        .when(pl.col("age_group") == "35-44\nYears Old").then(3)
        .when(pl.col("age_group") == "45-54\nYears Old").then(4)
        .when(pl.col("age_group") == "55-64\nYears Old").then(5)
        .when(pl.col("age_group") == "65+\nYears Old").then(6)
        .otherwise(7)
        .alias("age_group_order")
    )
    .group_by(["age_group", "age_group_order"])
    # Calculate the average mood and level progression scores by age groups
    .agg([
    pl.len().alias("n"),
    (pl.mean("response")/1000).alias("avg_mood"),
    pl.mean("LevelProgressionAmount").alias("avg_prog")
    ])
    .sort("age_group_order")
)

# Now want the average moods and level progression to be pivoted into one variable
# This way, the bars in the plot will be "side-by-side"
df_demo_moods_long = (
    df_demo_moods
    .unpivot(
        index = ["age_group", "age_group_order", "n"],
        on = ["avg_prog", "avg_mood"],
        variable_name = "metric",
        value_name="value"
    )
    .with_columns(
        pl.when(pl.col("metric") == "avg_prog")
        .then(pl.lit("Average Level Progress"))
        .when(pl.col("metric") == "avg_mood")
        .then(pl.lit("Average Reported Mood"))\
        .alias("metric"),
        pl.format("{}%", (pl.col("value") * 100).round(1))
        .alias("value_pct"),
        pl.format("{}\n\n(n = {})", pl.col("age_group"), pl.col("n"))
        .alias("age_group")
)
.sort("age_group_order")
)
#==========================================================================#
#%% Font Load-ins-----------------------------------------------------------
lst_fonts = ("fonts/Agdasima-Regular.ttf", "fonts/WorkSans-Medium.ttf")

dir_here = os.getcwd()

# Dictionary to store font names
font_names = {}

for font_file in lst_fonts:
    font_path = os.path.join(dir_here, font_file)
    fm.fontManager.addfont(font_path)
    
    # Get the font name and store it
    font_prop = fm.FontProperties(fname=font_path)
    font_name = font_prop.get_name()
    
    # Clean up the names for the keys to reference them easier
    key = os.path.splitext(os.path.basename(font_file))[0].split('-')[0]
    font_names[key] = font_name

# Rebuild cache once after all fonts are added
fm._load_fontmanager(try_read_cache=False)

#==========================================================================#

#%% Plot Creation-----------------------------------------------------------
background_color = "#033c70"
dodge_text = pn.position_dodge(width = 1.2)
description_text = "This visual shows average Power Wash Simulator player experiences by age group. Blue percentages represent average level progression,\nand green percentages represent self-reported well-being (N = 19,837). Research conducted by Tilburg University's School of Social and\nBehavioral Sciences using a research edition of Power Wash Simulator, a calming first-person powerwashing game."
pn.options.figure_size = (12, 8)

plt_top = (
    pn.ggplot()
    + pn.watermark("images/house.png", xo=0, yo=1000)
    + pn.theme_void() 
    + pn.theme(
        panel_background = pn.element_rect(fill = background_color),
        plot_background = pn.element_rect(fill = background_color),
    )
)

plt_bottom = (
    pn.ggplot(df_demo_moods_long, pn.aes("age_group", "value", fill="metric"))
    + pn.scale_fill_manual(("#3365b5", "#0b7337"))
    + pn.geom_col(
        mapping = pn.aes(y = 1),
        fill = "#bababa",
        position = "dodge",
        size = .2,
        width = .5
        )
    + pn.geom_point(
        mapping = pn.aes(y = 0.01),
        fill = "#bababa",
        size = 39,
        stroke = 0
        )
    + pn.geom_col(
        position = "dodge",
        color = "#ffffff",
        size = .2,
        width = .45
        )
    + pn.geom_label(
        mapping = pn.aes(label = "value_pct"),
        color = "#ffffff",
        size = 10,
        position = dodge_text,
        family = font_names["WorkSans"],
        show_legend = False
    )
    + pn.coord_cartesian(
        ylim = (-.4,1)
    )
    + pn.theme_void()
    + pn.theme(
    panel_background = pn.element_rect(fill=background_color),
    plot_background = pn.element_rect(fill=background_color),
    plot_margin = .01,
    axis_text_x = pn.element_text(
        color = "#ffffff",
        size = 15,
        family = font_names["Agdasima"],
        margin = {"t": 10, "units": "pt"},
        linespacing = 1.5 
    ),
    legend_text = pn.element_text(color="#ffffff"),
    legend_title = pn.element_blank(),
    legend_position = "top",
    plot_subtitle = pn.element_text(
        color="#D7DADD",
        size=12,
        family=font_names["WorkSans"],
        margin={"b": 20, "t": -10, "units": "pt"},
        ha="left",
        linespacing = 1.5 
    ),
    plot_caption = pn.element_text(
        color="#D7DADD",
        size=12,
        family=font_names["WorkSans"],
        margin={"t": 10, "units": "pt"} 
    )
    )
    + pn.guides(
        fill = pn.guide_legend(override_aes={'label': ''})
        )
    + pn.labs(
    subtitle = description_text,
    caption = "Graphic: Meghan S. Harris"
    )
)

# Final Plot
plt_top/plt_bottom
