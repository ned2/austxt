import os

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


@app.route("/", methods=['GET', 'POST'])
def index():
    form = QueryForm(request.form)
    if request.method == 'POST' and form.validate():
        for i in range(1, form.num_queries + 1):
            query = getattr(form, "query_{i}")
            query_type = getattr(form, "query_type_{i}")
            sen_dataset_df = make_dataset(SENATES_DF, query,
                                          config.DEFAULT_INDEX,
                                          config.ELASTIC_MAX_RESULTS,
                                          query_type)
            reps_dataset_df = make_dataset(REPRESENTATIVES_DF, query,
                                           config.DEFAULT_INDEX,
                                           config.ELASTIC_MAX_RESULTS,
                                           query_type)
    return render_template('templates/query.html', form=form)
