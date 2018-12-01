import spacy


nlp = spacy.load('en_core_web_sm', disable=['parser', 'tagger'])



def clean_speeches(speeches):
    speech_tuples = ((speech.text, speech) for speech in speeches) 
    pipe = nlp.pipe(speech_tuples, as_tuples=True, n_threads=1,
                    disable=['parser', 'ner'])
    for doc, speech in pipe:
        speech.cleaned_text = ' '.join(token.lemma_ for token in doc
                                if not (token.is_stop or token.is_punct or
                                        token.is_space))
