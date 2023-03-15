import re
from datetime import datetime
from config import TRANSFORMED_TEXT_COLUMN, PERCENTAGE_COLUMN, ORIGINAL_TEXT_COLUMN, CSV_READING_FILE, CSV_OUTPUT_FILE, \
    ENCODING_FORMAT, FUZZY_MATCH_PERCENTAGE
import pandas as pd
from pandas import DataFrame
import multiprocessing as mp
from fuzzywuzzy import fuzz


def divide_to_groups() -> list:
    """
    this function divide the dataframe that is read from CSV_READING_FILE, according to the columns:
    'ID_fake','AdmissionNumber_fake'. the function delete all duplicated rows
    it returns list of groups that each group is a dataframe.
    :return:list(Dataframe)
    """
    print(datetime.now())
    df = pd.read_csv(CSV_READING_FILE)
    df = df.drop_duplicates(["Output_Text", "DocumentingTime", "ID_fake"])
    gb = df.sort_values(by=['DocumentingTime']).groupby(['ID_fake', "AdmissionNumber_fake"])
    return [gb.get_group(x) for x in gb.groups]


def apply_remove_duplicates(group) -> DataFrame:
    new_groups = [remove_duplicates(group)]
    return pd.concat(new_groups)


def remove_duplicates(group: DataFrame) -> DataFrame:
    """
    this function removes all the duplicated text in the dataframe according to fuzzy matching, the score is configurable
    """
    group[TRANSFORMED_TEXT_COLUMN] = group[ORIGINAL_TEXT_COLUMN]
    group['sentences'] = group[ORIGINAL_TEXT_COLUMN].str.split(r'(?<!\d)\.\s+(?!\d)')
    for i, row in group.iterrows():
        if type(row["sentences"]) != float:
            for sentence in row["sentences"]:
                if len(sentence) > 0:
                    for j, other_row in group.iterrows():
                        if j <= i:
                            continue
                        if type(other_row["sentences"]) != float:
                            for other_sentence in other_row["sentences"]:
                                if len(other_sentence) > 0 and fuzz.ratio(sentence.strip().replace(r'\W+', ''),
                                                                          other_sentence.strip().replace(r'\W+',
                                                                                                         '')) > FUZZY_MATCH_PERCENTAGE:
                                    group.at[j, TRANSFORMED_TEXT_COLUMN] = other_row[TRANSFORMED_TEXT_COLUMN].replace(
                                        other_sentence, '')
    return group.drop("sentences", axis='columns')


def clean_up_empty_boxes(df: DataFrame):
    """
    after removing text duplication there are a lot of empty text just with dots. this function removes them
    """
    df[TRANSFORMED_TEXT_COLUMN] = df[TRANSFORMED_TEXT_COLUMN].apply(
        lambda freeText: re.sub(r' +\. +| +\.|\. +\.', ' ', str(freeText)) if isinstance(freeText, str) else freeText)
    df[TRANSFORMED_TEXT_COLUMN] = df[TRANSFORMED_TEXT_COLUMN].apply(
        lambda freeText: re.sub(r'\B\s+\B|\B\.\B', '', str(freeText)) if isinstance(freeText, str) else freeText)
    return df


def limit_100(x):
    if float(x) > 100:
        return "100"
    return x


def add_percentage_column(df: DataFrame) -> DataFrame:
    """
    add a column of percentages, the percentage is how much of the text remained after cleaning duplications
    """

    df[PERCENTAGE_COLUMN] = df.apply(
        lambda row: round(len(re.split(r'[א-ת] +[א-ת]|[\.א-ת] +[א-ת]', row[TRANSFORMED_TEXT_COLUMN]) if row[
                                                                                                            TRANSFORMED_TEXT_COLUMN] and isinstance(
            row[TRANSFORMED_TEXT_COLUMN], str) else [])
                          / len(
            re.split(r'[א-ת] +[א-ת]|[\.א-ת] +[א-ת]', row[ORIGINAL_TEXT_COLUMN]) if row[ORIGINAL_TEXT_COLUMN] and type(
                row[ORIGINAL_TEXT_COLUMN]) != float and isinstance(row[TRANSFORMED_TEXT_COLUMN], str) else ['']) * 100,
                          2), axis=1)
    df[PERCENTAGE_COLUMN] = df[PERCENTAGE_COLUMN].apply(limit_100)
    return df


def worker(group, queue):
    queue.put(apply_remove_duplicates(group))


def drop_indices_row(df: DataFrame):
    """
    the program create 2 columns of indexes that aren't needed, this function removes them
    """
    df = df.drop(df.columns[[0, 1]], axis=1)
    return df


def activate():
    """
    this function activates the flow of the program and uses multiprocessing to do it in parallel for each group,
     each group represents a hospitalization of a single patient. return changed csv file in the output path
    :return:
    """
    num_processes = mp.cpu_count()
    pool = mp.Pool(processes=num_processes)
    manager = mp.Manager()
    queue = manager.Queue()
    results = []
    for group in divide_to_groups():
        results.append(pool.apply_async(worker, args=(group, queue)))
    pool.close()
    pool.join()

    groups = []
    while not queue.empty():
        groups.append(queue.get())

    combined = pd.concat(groups)
    drop_indices_row(add_percentage_column(clean_up_empty_boxes(combined))).to_csv(CSV_OUTPUT_FILE,
                                                                                   encoding=ENCODING_FORMAT)
    print(datetime.now())


if __name__ == '__main__':
    activate()
