from fuzzywuzzy import fuzz
from pandas import DataFrame
from config import FUZZY_SUBTYPE_MATCH_PERCENTAGE
from fuzzy_words import FuzzyWords


def fix_typos_on_subtype_description(df: DataFrame) -> DataFrame:
    df['Subtype_Description'] = df['Subtype_Description'].apply(_replace_similarities, axis=1)
    return df


def _replace_similarities(row):
    for word in FuzzyWords.value:
        if fuzz.ratio(row['Subtype_Description'], word) > FUZZY_SUBTYPE_MATCH_PERCENTAGE:
            return word
    return row['Subtype_Description']

