import re
import pandas as pd


def process_query_result(result):
    results = []
    for hit in result['hits']['hits']:
        doc_id = hit['_id']
        tf_string = hit['_explanation']['details'][0]['description']
        tf = int(re.search(r'termFreq=(\d+)\.', tf_string)[1])
        results.append((doc_id, tf))
    return results


def add_members_columns(speeches_df, members_path, columns):
    members_df = pd.read_csv(members_path, usecols=['member_id']+columns)
    new_df = speeches_df.merge(members_df, left_on='speaker_id', right_on='member_id',
                               how='left').drop('member_id', axis=1)
    return new_df 


def add_results_to_dataframe(parsed_results, dataframe, column_name):
    id_col = "speech_id"
    extra_col_df = pd.DataFrame.from_records(
        parsed_results,
        columns=[id_col, column_name]
    )
    new_df = dataframe.merge(extra_col_df, on=id_col)
    breakpoint()
    # next steps:
    #  -- make the id of the CSV files include the sen_ or rep_ prefix 
    #  -- will need to add speech_type argument to process_debates

    #  -- also add exact_text field with {"type": "keyword"} in order to support exact
    #     matching
    #  -- then re-index with new prefixes, for the join to work
   
    return new_df
