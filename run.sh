export FLASK_APP=austxt.app.app
export AUSTXT_DATA_PATH=${HOME}/data/austxt
export PYTHONBREAKPOINT=ipdb.set_trace

flask run --host 0.0.0.0
