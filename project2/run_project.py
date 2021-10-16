from tqdm import tqdm
from preprocessor import Preprocessor
from indexer import Indexer
from collections import OrderedDict
from linkedlist import LinkedList
import inspect as inspector
import sys
import argparse
import json
import time
import random
import flask
from flask import Flask
from flask import request
import hashlib
import math

app = Flask(__name__)


class ProjectRunner:
    def __init__(self):
        self.preprocessor = Preprocessor()
        self.indexer = Indexer()

    def _merge(self, list1, list2):
        """ Implement the merge algorithm to merge 2 postings list at a time.
            Use appropriate parameters & return types.
            While merging 2 postings list, preserve the maximum tf-idf value of a document.
            To be implemented."""
        num_comparision = 0
        results = []
        ind1, ind2 = 0, 0

        while ind1 < len(list1) and ind2 < len(list2):
            if list1[ind1] == list2[ind2]:
                results.append(list1[ind1])
                ind1 += 1
                ind2 += 1
            elif list1[ind1] < list2[ind2]:
                ind1 += 1
            else:
                ind2 += 1
            num_comparision += 1
        return results, num_comparision

    def _merge_skip(self, list1, list2):
        num_comparision = 0
        results = []
        ind1, ind2 = 0, 0
        skip_length1 = int(round(math.sqrt(len(list1)), 0))
        skip_length2 = int(round(math.sqrt(len(list2)), 0))

        cur_skip1 = 0
        cur_skip2 = 0
        while ind1 < len(list1) and ind2 < len(list2):
            if list1[ind1] == list2[ind2]:
                results.append(list1[ind1])
                ind1 += 1
                ind2 += 1
                num_comparision += 1

            elif list1[ind1] < list2[ind2]:
                if (ind1%skip_length1 == 0 and ind1+skip_length1 < len(list1)) and list1[ind1 + skip_length1] < list2[ind2]:
                    #num_comparision += 1
                    while (ind1%skip_length1 == 0 and ind1+skip_length1 < len(list1)) and list1[ind1 + skip_length1] < list2[ind2]:
                        cur_skip1 += 1
                        ind1 = cur_skip1*skip_length1
                        num_comparision += 1
                else:
                    ind1 += 1
                    num_comparision += 1
            else:
                if (ind2%skip_length2 == 0 and ind2+skip_length2 <len(list2)) and list2[ind2 + skip_length2] < list1[ind1]:
                    #num_comparision += 1
                    while (ind2%skip_length2 == 0 and ind2+skip_length2 <len(list2)) and list2[ind2 + skip_length2] < list1[ind1]:
                        cur_skip2 += 1
                        ind2 = cur_skip2*skip_length2
                        num_comparision += 1
                else:
                    ind2 += 1
                    num_comparision += 1

        return results, num_comparision

    def _daat_and_skip(self, query_term_posting, query_term_skip_posting):
        query_term_posting = dict(sorted(query_term_posting.items(), key=lambda x: len(x[1]), reverse=False))
        query_term_skip_posting = dict(sorted(query_term_skip_posting.items(), key=lambda x: len(x[1]), reverse=False))

        first_key = list(query_term_posting.keys())[0]
        results = []
        num_comparision = 0

        current_results = query_term_posting[first_key]
        current_results_skip = query_term_skip_posting[first_key]


        for index, key in enumerate(query_term_posting):
            if index == 0:
                continue
            second_list = query_term_posting[key]
            current_results, comparision = self._merge_skip(current_results, second_list)
            num_comparision += comparision

        results = current_results

        return results, num_comparision

    def _daat_and_tf_idf(self, input_term_arr, query_term_posting, query_term_skip_posting):
        document = set()
        query_term_tf_posting ={}
        for term in input_term_arr:
            for doc in query_term_posting[term]:
                document.add(doc)

            query_term_tf_posting[term] = None
            if term in self.indexer.get_index().keys():
                query_term_tf_posting[term] = self.indexer.get_index()[term].traverse_tf_score()

        results = []
        document_score = {}
        for doc in document:
            doc_score = 0.0
            term_cnt = 0
            match_cnt =0
            for term in input_term_arr:
                term_cnt += 1
                posting_list = query_term_posting[term]
                posting_tf_score = query_term_tf_posting[term]

                for p_list, p_tf_score in zip(posting_list, posting_tf_score):
                    if p_list == doc:
                        match_cnt += 1
                        #doc_score += self.indexer.get_index()[term].get_idf()
                        doc_score += (self.indexer.get_index()[term].get_idf() * p_tf_score)
                        break
            if term_cnt == match_cnt:
                document_score[doc] = doc_score

        document_score = OrderedDict(sorted(document_score.items(), key=lambda x: (-x[1], x[0])))
        print(document_score)

        for doc in document_score.keys():
            results.append(doc)

        return results


    def _daat_and(self, input_term_arr):
        """ Implement the DAAT AND algorithm, which merges the postings list of N query terms.
            Use appropriate parameters & return types.
            To be implemented."""

        query_term_posting = {}
        query_term_skip_posting = {}
        for term in input_term_arr:
            query_term_posting[term], query_term_skip_posting[term] = None, None
            if term in self.indexer.get_index().keys():
                query_term_posting[term] = self.indexer.get_index()[term].traverse_list()
                query_term_skip_posting[term] = self.indexer.get_index()[term].traverse_skips()

        query_term_posting = dict(sorted(query_term_posting.items(), key=lambda x: len(x[1]), reverse=False))
        query_term_skip_posting = dict(sorted(query_term_skip_posting.items(), key=lambda x: len(x[1]), reverse=False))

        first_key = list(query_term_posting.keys())[0]
        results = []
        num_comparision = 0

        current_results = query_term_posting[first_key]
        for index, key in enumerate(query_term_posting):
            if index == 0:
                continue
            second_list = query_term_posting[key]
            current_results, comparision = self._merge(current_results, second_list)
            num_comparision += comparision

        results = current_results

        skip_results, skip_comparision= self._daat_and_skip(query_term_posting, query_term_skip_posting)
        return results, num_comparision, skip_results, skip_comparision

    def _get_postings(self, term):
        """ Function to get the postings list of a term from the index.
            Use appropriate parameters & return types.
            To be implemented."""
        return self.indexer.get_index()[term].traverse_list()

    def _output_formatter(self, op):
        """ This formats the result in the required format.
            Do NOT change."""
        if op is None or len(op) == 0:
            return [], 0
        op_no_score = [int(i) for i in op]
        results_cnt = len(op_no_score)
        return op_no_score, results_cnt

    def run_indexer(self, corpus):
        """ This function reads & indexes the corpus. After creating the inverted index,
            it sorts the index by the terms, add skip pointers, and calculates the tf-idf scores.
            Already implemented, but you can modify the orchestration, as you seem fit."""
        with open(corpus, 'r') as fp:
            for line in tqdm(fp.readlines()):
                doc_id, document = self.preprocessor.get_doc_id(line)
                tokenized_document = self.preprocessor.tokenizer(document)
                self.indexer.generate_inverted_index(doc_id, tokenized_document)
        self.indexer.sort_terms()
        self.indexer.add_skip_connections()
        self.indexer.calculate_tf_idf()

    def sanity_checker(self, command):
        """ DO NOT MODIFY THIS. THIS IS USED BY THE GRADER. """

        index = self.indexer.get_index()
        kw = random.choice(list(index.keys()))
        return {"index_type": str(type(index)),
                "indexer_type": str(type(self.indexer)),
                "post_mem": str(index[kw]),
                "post_type": str(type(index[kw])),
                "node_mem": str(index[kw].start_node),
                "node_type": str(type(index[kw].start_node)),
                "node_value": str(index[kw].start_node.value),
                "command_result": eval(command) if "." in command else ""}

    def run_queries(self, query_list, random_command):
        """ DO NOT CHANGE THE output_dict definition"""
        output_dict = {'postingsList': {},
                       'postingsListSkip': {},
                       'daatAnd': {},
                       'daatAndSkip': {},
                       'daatAndTfIdf': {},
                       'daatAndSkipTfIdf': {},
                       'sanity': self.sanity_checker(random_command)}

        for query in tqdm(query_list):
            """ Run each query against the index. You should do the following for each query:
                1. Pre-process & tokenize the query.
                2. For each query token, get the postings list & postings list with skip pointers.
                3. Get the DAAT AND query results & number of comparisons with & without skip pointers.
                4. Get the DAAT AND query results & number of comparisons with & without skip pointers, 
                    along with sorting by tf-idf scores."""
            #raise NotImplementedError

            input_term_arr = self.preprocessor.tokenizer(query)  # Tokenized query. To be implemented.

            query_term_posting = {}
            query_term_skip_posting = {}
            for term in input_term_arr:
                postings, skip_postings = None, None

                if term in self.indexer.get_index().keys():
                    postings = self.indexer.get_index()[term].traverse_list()
                    skip_postings = self.indexer.get_index()[term].traverse_skips()

                """ Implement logic to populate initialize the above variables.
                    The below code formats your result to the required format.
                    To be implemented."""

                output_dict['postingsList'][term] = postings
                output_dict['postingsListSkip'][term] = skip_postings

                query_term_posting[term] = postings
                query_term_skip_posting[term] = skip_postings

           # print(query, query_term_posting.keys())

            #break
            and_op_no_skip, and_op_skip, and_op_no_skip_sorted, and_op_skip_sorted = None, None, None, None
            and_comparisons_no_skip, and_comparisons_skip, \
                and_comparisons_no_skip_sorted, and_comparisons_skip_sorted = None, None, None, None
            """ Implement logic to populate initialize the above variables.
                The below code formats your result to the required format.
                To be implemented."""
            and_op_no_skip, and_comparisons_no_skip, and_op_skip, and_comparisons_skip = self._daat_and(input_term_arr)
            and_op_no_skip_sorted = self._daat_and_tf_idf(input_term_arr, query_term_posting, query_term_skip_posting)
            and_op_skip_sorted = and_op_no_skip_sorted

            and_comparisons_no_skip_sorted = and_comparisons_no_skip
            and_comparisons_skip_sorted = and_comparisons_skip


            #and_op_skip, and_comparisons_skip = self._daat_and_skip(query_term_posting, query_term_skip_posting)


            and_op_no_score_no_skip, and_results_cnt_no_skip = self._output_formatter(and_op_no_skip)
            and_op_no_score_skip, and_results_cnt_skip = self._output_formatter(and_op_skip)
            and_op_no_score_no_skip_sorted, and_results_cnt_no_skip_sorted = self._output_formatter(and_op_no_skip_sorted)
            and_op_no_score_skip_sorted, and_results_cnt_skip_sorted = self._output_formatter(and_op_skip_sorted)

            output_dict['daatAnd'][query.strip()] = {}
            output_dict['daatAnd'][query.strip()]['results'] = and_op_no_score_no_skip
            output_dict['daatAnd'][query.strip()]['num_docs'] = and_results_cnt_no_skip
            output_dict['daatAnd'][query.strip()]['num_comparisons'] = and_comparisons_no_skip

            output_dict['daatAndSkip'][query.strip()] = {}
            output_dict['daatAndSkip'][query.strip()]['results'] = and_op_no_score_skip
            output_dict['daatAndSkip'][query.strip()]['num_docs'] = and_results_cnt_skip
            output_dict['daatAndSkip'][query.strip()]['num_comparisons'] = and_comparisons_skip

            output_dict['daatAndTfIdf'][query.strip()] = {}
            output_dict['daatAndTfIdf'][query.strip()]['results'] = and_op_no_score_no_skip_sorted
            output_dict['daatAndTfIdf'][query.strip()]['num_docs'] = and_results_cnt_no_skip_sorted
            output_dict['daatAndTfIdf'][query.strip()]['num_comparisons'] = and_comparisons_no_skip_sorted

            output_dict['daatAndSkipTfIdf'][query.strip()] = {}
            output_dict['daatAndSkipTfIdf'][query.strip()]['results'] = and_op_no_score_skip_sorted
            output_dict['daatAndSkipTfIdf'][query.strip()]['num_docs'] = and_results_cnt_skip_sorted
            output_dict['daatAndSkipTfIdf'][query.strip()]['num_comparisons'] = and_comparisons_skip_sorted

            print(output_dict['daatAndTfIdf'])
            #break

        #output_dict = open('data/sample_output.json')
        #output_dict = (json.load(output_dict))
        return output_dict


@app.route("/execute_query", methods=['POST'])
def execute_query():
    """ This function handles the POST request to your endpoint.
        Do NOT change it."""
    start_time = time.time()

    queries = request.json["queries"]
    random_command = request.json["random_command"]

    """ Running the queries against the pre-loaded index. """
    output_dict = runner.run_queries(queries, random_command)

    """ Dumping the results to a JSON file. """
    with open(output_location, 'w') as fp:
        json.dump(output_dict, fp)

    response = {
        "Response": output_dict,
        "time_taken": str(time.time() - start_time),
        "username_hash": username_hash
    }
    return flask.jsonify(response)


if __name__ == "__main__":
    """ Driver code for the project, which defines the global variables.
        Do NOT change it."""

    output_location = "project2_output.json"
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--corpus", type=str, help="Corpus File name, with path.")
    parser.add_argument("--output_location", type=str, help="Output file name.", default=output_location)
    parser.add_argument("--username", type=str,
                        help="Your UB username. It's the part of your UB email id before the @buffalo.edu. "
                             "DO NOT pass incorrect value here")

    argv = parser.parse_args()

    corpus = argv.corpus
    output_location = argv.output_location
    username_hash = hashlib.md5(argv.username.encode()).hexdigest()

    """ Initialize the project runner"""
    runner = ProjectRunner()

    """ Index the documents from beforehand. When the API endpoint is hit, queries are run against 
        this pre-loaded in memory index. """
    runner.run_indexer(corpus)

    query_list = ["the novel coronavirus", "from an epidemic to a pandemic", "is hydroxychloroquine effective?"]
    # query_list = ["hello world", "hello swimming", "swimming going", "random swimming"]
    random_command = "self.indexer.get_index()['random'].traverse_list()"
    runner.run_queries(query_list, random_command)

    #app.run(host="0.0.0.0", port=9999)
