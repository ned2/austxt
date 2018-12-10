import os
import tempfile

import pandas as pd
from flask import Flask, render_template, request
from wtforms import Form, StringField, SelectField, validators

from .. import config
from ..utils import make_dataset


app = Flask(__name__)

SENATES_DF = pd.read_csv(config.SENATES_CSV_PATH)
REPRESENTATIVES_DF = pd.read_csv(config.REPRESENTATIVES_CSV_PATH)


def make_query_form(num_queries=10):
    class FormClass(Form):
        num_queries = num_queries
        
    query_type_choices = [("and", "and"), ("or", "or"), ("exact", "exact")]
    for i in range(1, num_queries +1):
        if i == 1:
            validators = [validators.required()]
        else:
            validators = [validators.optional()]
            
        query = f"query_{i}"
        query_type = f"query_type_{i}"
        setattr(FormClass, query, TextField(query), validators=validators)
        setattr(FormClass, query_type, SelectField(query_type,
                                                   validators=validators,
                                                   default=query_type_choices[0],
                                                   choices=query_type_choices))
    return FormClass


QueryForm = make_query_form()


def get_download_path(basepath=config.DOWNLOAD_PATH):
    Path(basepath).mkdir(exist_ok=True)
    return Path(tempfile.mkdtemp(dir=basepath))


@app.route("/", methods=['GET', 'POST'])
def index():
    form = QueryForm(request.form)
    if request.method == 'POST' and form.validate():
        savepath = get_download_path()
        try:
            # initialise these to the original dataset, then for each query, add
            # a column with the results of the query
            sen_dataset_df = SENATES_DF
            reps_dataset_df = REPRESENTATIVES_DF

            for i in range(1, form.num_queries + 1):
                query = getattr(form, "query_{i}")
                query_type = getattr(form, "query_type_{i}")
                sen_dataset_df = make_dataset(sen_dataset_df, query,
                                              config.DEFAULT_INDEX,
                                              config.ELASTIC_MAX_RESULTS,
                                              query_type)
                reps_dataset_df = make_dataset(reps_dataset_df, query,
                                               config.DEFAULT_INDEX,
                                               config.ELASTIC_MAX_RESULTS,
                                               query_type)
            except Exception as error:
                # Clean up directory that was created
                savepath.rmdir()
                raise error
        sen_path = savepath / "senate_speeches_query.csv.gzip"
        reps_path = savepath / "representatives_speeches_query.csv.gzip"
        sen_dataset_df.to_csv(sen_path, compress="gzip")
        reps_dataset_df.to_csv(reps_path, compress="gzip")
        return render_template('templates/query.html', form=form,
                               sen_path=sen_path, reps_path=reps_path)

    return render_template('templates/query.html', form=form)
