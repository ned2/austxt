import os

import pandas as pd
from flask import Flask, render_template, request, abort, send_from_directory
from werkzeug.security import safe_join

from .utils import get_download_path, make_query_form
from .. import config
from ..utils import (process_query_result, query_to_column_name,
                     add_results_to_dataframe)
from ..elastic import global_elastic, do_query


app = Flask(__name__)
QueryForm = make_query_form()


@app.before_first_request
def run_on_start():
    global SENATES_DF
    global REPRESENTATIVES_DF
    SENATES_DF = pd.read_csv(config.SENATES_CSV_PATH)
    REPRESENTATIVES_DF = pd.read_csv(config.REPRESENTATIVES_CSV_PATH)

# TODO:
# -- style the page
# -- static page as a backup
# -- gender column for people
# -- pages/slides to show off data on the night
# -- quick graph of the results
# -- cleaned text column
# -- sentiment column?
    
@app.route("/", methods=['GET', 'POST'])
def index():
    form = QueryForm(request.form)
    if not (request.method == 'POST' and form.validate()):
        return render_template('query.html', form=form)

    savepath = get_download_path()
    try:
        # initialise these to the original dataset, then for each query, add
        # a column with the results of the query
        sen_df = SENATES_DF
        reps_df = REPRESENTATIVES_DF
        for i in range(1, form.num_queries + 1):
            query = getattr(form, f"query_{i}").data
            query_type = getattr(form, f"query_type_{i}").data
            if query == '' or query_type == '':
                continue
            results = do_query(query, config.DEFAULT_INDEX,
                               config.ELASTIC_MAX_RESULTS, query_type)
            column_name = query_to_column_name(query, query_type)
            parsed_results = process_query_result(results)
            sen_df = add_results_to_dataframe(parsed_results, sen_df, column_name)
            reps_df = add_results_to_dataframe(parsed_results, reps_df, column_name)
    except Exception as error:
        # Clean up directory that was created
        savepath.rmdir()
        raise error
    sen_path = savepath / "senate_speeches_query.csv.gz"
    reps_path = savepath / "representatives_speeches_query.csv.gz"
    sen_df.to_csv(sen_path, compression="gzip")
    reps_df.to_csv(reps_path, compression="gzip")
    return render_template('query.html', form=form,
                           sen_path=sen_path, reps_path=reps_path)


@app.route(f"/download/<tmp_dir>/<path:filename>")
def download_file(filename, tmp_dir):
    path = safe_join(os.path.join(config.DOWNLOAD_PATH.resolve(), tmp_dir))
    if path is None:
        abort(404)
    return send_from_directory(path, filename, as_attachment=True)
