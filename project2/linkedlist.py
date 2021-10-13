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
        self.token_freq = 0
        self.token_cnt = 0


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
        while n is not None:
            n = n.next
            cnt += 1
            if cnt == self.skip_length:
                cnt = 0
                cur_skip_node.next_skip = n
                cur_skip_node = cur_skip_node.next_skip
        
        if cnt > 0:
            cur_skip_node.next_skip = n
            cur_skip_node = cur_skip_node.next_skip
        return 
        raise NotImplementedError

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
            return

        elif self.end_node.value <= value:
            if self.end_node.value < value:
                self.end_node.next = new_node
                self.end_node = new_node
                self.length = self.length + 1
            return

        else:
            while(current_node.next is not None and current_node.next.value < new_node.value):
                current_node = current_node.next
            
            if new_node.value < current_node.next.value:
                new_node.next = current_node.next
                current_node.next = new_node
                self.length = self.length + 1
            return
        raise NotImplementedError

