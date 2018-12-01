import pandas as pd


def add_members_columns(speeches_df, members_path, columns):
    members_df = pd.read_csv(members_path, usecols=['member_id']+columns)
    new_df = speeches_df.merge(members_df, left_on='speaker_id', right_on='member_id',
                               how='left').drop('member_id', axis=1)
    return new_df 
