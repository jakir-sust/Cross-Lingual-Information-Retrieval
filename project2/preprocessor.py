import collections
from nltk.stem import PorterStemmer
import re
from nltk.corpus import stopwords
import nltk
nltk.download('stopwords')


class Preprocessor:
    def __init__(self):
        self.stop_words = set(stopwords.words('english'))
        self.ps = PorterStemmer()

    def get_doc_id(self, doc):
        """ Splits each line of the document, into doc_id & text.
            Already implemented"""
        arr = doc.split("\t")
        return int(arr[0]), arr[1]

    def tokenizer(self, text):
        """ Implement logic to pre-process & tokenize document text.
            Write the code in such a way that it can be re-used for processing the user's query.
            To be implemented."""
        
        #print("Original ---->>>>  ", text)
        text = text.lower()
        #text = re.sub(r'[-]+', ' ', text)
        text = re.sub(r'[^A-Za-z0-9 ]+', ' ', text)
        text = re.sub(' +', ' ', text)
        tokenized_text = text.split()

        tokenized_text = [word for word in tokenized_text if word not in self.stop_words]
        tokenized_text = [self.ps.stem(word) for word in tokenized_text]

        #print(tokenized_text)

        return tokenized_text

        raise NotImplementedError
