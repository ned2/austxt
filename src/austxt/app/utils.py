import tempfile
from pathlib import Path

from wtforms import Form, StringField, SelectField, validators

from .. import config


def get_download_path(basepath=config.DOWNLOAD_PATH):
    Path(basepath).mkdir(exist_ok=True)
    return Path(tempfile.mkdtemp(dir=basepath))


def make_query_form(num_queries=10):
    class FormClass(Form):
        def get_query(self, i):
            query_field = getattr(self, f"query_{i}") 
            query_type_field = getattr(self, f"query_type_{i}") 
            return query_field, query_type_field 
    
    FormClass.num_queries = num_queries
    FormClass.queries = []
    query_type_choices = [("and", "and"), ("or", "or"), ("exact", "exact")]
    for i in range(1, num_queries +1):
        if i == 1:
            query_validators = [validators.required()]
        else:
            query_validators = [validators.optional()]
            
        query = f"query_{i}"
        query_type = f"query_type_{i}"
        query_field = StringField(query, validators=query_validators)
        query_type_field = SelectField(query_type, validators=query_validators,
                                       default=query_type_choices[0],
                                       choices=query_type_choices)
        setattr(FormClass, query, query_field)
        setattr(FormClass, query_type, query_type_field)
    return FormClass


