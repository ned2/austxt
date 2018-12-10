import os
import tempfile
from pathlib import Path

import pandas as pd
from flask import Flask, render_template, request, abort, send_from_directory
from werkzeug.security import safe_join

from .utils import get_download_path, make_query_form
from .. import config
from ..utils import make_dataset
from ..elastic import global_elastic


app = Flask(__name__)
QueryForm = make_query_form()


@app.before_first_request
def run_on_start():
    global SENATES_DF
    global REPRESENTATIVES_DF
    SENATES_DF = pd.read_csv(config.SENATES_CSV_PATH)
    REPRESENTATIVES_DF = pd.read_csv(config.REPRESENTATIVES_CSV_PATH)

    
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
                query = getattr(form, f"query_{i}").data
                query_type = getattr(form, f"query_type_{i}").data
                if query == '' or query_type == '':
                    continue
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
        sen_path = savepath / "senate_speeches_query.csv.gz"
        reps_path = savepath / "representatives_speeches_query.csv.gz"
        sen_dataset_df.to_csv(sen_path, compression="gzip")
        reps_dataset_df.to_csv(reps_path, compression="gzip")
        print(sen_path)
        return render_template('query.html', form=form,
                               sen_path=sen_path, reps_path=reps_path)

    return render_template('query.html', form=form)


@app.route(f"/download/<tmp_dir>/<path:filename>")
def download_file(filename, tmp_dir):
    path = safe_join(os.path.join(config.DOWNLOAD_PATH, tmp_dir))
    if path is None:
        abort(404)
    return send_from_directory(path, filename, as_attachment=True)
