import spacy


nlp = spacy.load('en_core_web_sm', disable=['parser', 'tagger'])


def clean_text(text):
    return ' '.join(token.lower_ for token in nlp(text)
                    if not (token.is_stop or token.is_punct or token.is_space))
