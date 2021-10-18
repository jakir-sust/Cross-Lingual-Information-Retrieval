import math


class Node:

    def __init__(self, value=None, next=None):
        """ Class to define the structure of each node in a linked list (postings list).
            Value: document id, Next: Pointer to the next node
            Add more parameters if needed.
            Hint: You may want to define skip pointers & appropriate score calculation here"""
        self.value = value
        self.next = next
        self.next_skip = next
        self.token_freq = 1
        self.tf_score = 0.0
        self.has_skip = 0


class LinkedList:
    """ Class to define a linked list (postings list). Each element in the linked list is of the type 'Node'
        Each term in the inverted index has an associated linked list object.
        Feel free to add additional functions to this class."""
    def __init__(self):
        self.start_node = None
        self.end_node = None
        self.length, self.n_skips, self.idf = 0, 0, 0.0
        self.skip_length = None

    def traverse_list(self):
        traversal = []
        if self.start_node is None:
            return
        else:
            """ Write logic to traverse the linked list.
                To be implemented."""
            n = self.start_node
            # Start traversal from head, and go on till you reach None
            while n is not None:
                traversal.append(n.value)
                n = n.next
            return traversal

            raise NotImplementedError

    def traverse_skips(self):
        traversal = []
        if self.start_node is None:
            return
        else:
            """ Write logic to traverse the linked list using skip pointers.
                To be implemented."""
            n = self.start_node
            # Start traversal from head, and go on till you reach None
            while n is not None:
                if n.has_skip:
                    traversal.append(n.value)
                n = n.next_skip
            return traversal
            raise NotImplementedError

    def add_skip_connections(self):
        self.n_skips = math.floor(math.sqrt(self.length))
        if self.n_skips * self.n_skips == self.length:
            self.n_skips = self.n_skips - 1
        """ Write logic to add skip pointers to the linked list. 
            This function does not return anything.
            To be implemented."""
        n = self.start_node
        cur_skip_node = self.start_node
        self.skip_length = int(round(math.sqrt(self.length), 0))
        cnt = 0
        cur_skip_node.has_skip = 1
        while n is not None:
            n = n.next
            cnt += 1
            if cnt == self.skip_length:
                #if n is not None:
                cur_skip_node.has_skip = 1
                cnt = 0
                cur_skip_node.next_skip = n
                cur_skip_node = cur_skip_node.next_skip
        
        if cnt > 0:
            #if n is not None:
             #   cur_skip_node.next_skip = n
            cur_skip_node.has_skip = 1
            cur_skip_node = cur_skip_node.next_skip
        return

    def insert_at_end(self, value):
        """ Write logic to add new elements to the linked list.
            Insert the element at an appropriate position, such that elements to the left are lower than the inserted
            element, and elements to the right are greater than the inserted element.
            To be implemented. """
        new_node = Node(value=value)
        current_node = self.start_node

        if self.start_node is None:
            self.start_node = new_node
            self.end_node = new_node
            self.length = self.length + 1
            return

        elif self.start_node.value >= value:
            if self.start_node.value > value:
                self.start_node = new_node
                self.start_node.next = current_node
                self.length = self.length + 1
            else:
                self.start_node.token_freq += 1
            return

        elif self.end_node.value <= value:
            if self.end_node.value < value:
                self.end_node.next = new_node
                self.end_node = new_node
                self.length = self.length + 1
            else:
                self.end_node.token_freq += 1
            return

        else:
            while(current_node.next is not None and current_node.next.value < new_node.value):
                current_node = current_node.next
            
            if new_node.value < current_node.next.value:
                new_node.next = current_node.next
                current_node.next = new_node
                self.length = self.length + 1
            else:
                current_node.next.token_freq += 1
            return
        raise NotImplementedError

    def traverse_tf_score(self):
        traversal = []
        if self.start_node is None:
            return
        else:
            """ Write logic to traverse the linked list.
                To be implemented."""
            n = self.start_node
            # Start traversal from head, and go on till you reach None
            while n is not None:
                traversal.append(n.tf_score)
                n = n.next
            return traversal

    def get_idf(self):
        return self.idf

    def assign_tf_idf_score(self, term, dic_token_count, total_document):

        #print("++++++++++++++++++++++++++++++++++++++++++++++++=       ", total_document)
        n = self.start_node
        cnt = 0
        while n is not None:
            n.tf_score = 1.0 * n.token_freq / dic_token_count[n.value]
            n = n.next
        self.idf = total_document / self.length


        #print(term, self.idf)

