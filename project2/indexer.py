from linkedlist import LinkedList
from collections import OrderedDict


class Indexer:
    def __init__(self):
        """ Add more attributes if needed"""
        self.inverted_index = OrderedDict({})
        self.dic_token_count = OrderedDict({})
        self.total_document = set() 

    def get_index(self):
        """ Function to get the index.
            Already implemented."""
        return self.inverted_index

    def generate_inverted_index(self, doc_id, tokenized_document):
        """ This function adds each tokenized document to the index. This in turn uses the function add_to_index
            Already implemented."""
        
        uniqueWords = []
        self.total_document.add(doc_id)
        for t in tokenized_document:
            self.add_to_index(t, doc_id)
            #if t not in uniqueWords:
            uniqueWords.append(t)
        
        self.dic_token_count[doc_id] = len(uniqueWords)

    def add_to_index(self, term_, doc_id_):
        """ This function adds each term & document id to the index.
            If a term is not present in the index, then add the term to the index & initialize a new postings list (linked list).
            If a term is present, then add the document to the appropriate position in the posstings list of the term.
            To be implemented."""

        if term_ not in self.inverted_index.keys():
            lkd_list = LinkedList()
            lkd_list.insert_at_end(doc_id_)
            self.inverted_index[term_] = lkd_list
        else:
            lkd_list = self.inverted_index[term_]
            lkd_list.insert_at_end(doc_id_)

        #raise NotImplementedError

    def sort_terms(self):
        """ Sorting the index by terms.
            Already implemented."""
        sorted_index = OrderedDict({})
        for k in sorted(self.inverted_index.keys()):
            sorted_index[k] = self.inverted_index[k]
        self.inverted_index = sorted_index

    def add_skip_connections(self):
        """ For each postings list in the index, add skip pointers.
            To be implemented."""
        for term in self.get_index().keys():
            lkd_list = self.inverted_index[term]
            lkd_list.add_skip_connections()

        #print("Skip connection added")
        return
        raise NotImplementedError

    def calculate_tf_idf(self):
        """ Calculate tf-idf score for each document in the postings lists of the index.
            To be implemented."""
       # print(len(self.total_document))
        for term in self.get_index().keys():
            lkd_list = self.inverted_index[term]
            lkd_list.assign_tf_idf_score(term, self.dic_token_count, len(self.total_document))

        #raise NotImplementedError
