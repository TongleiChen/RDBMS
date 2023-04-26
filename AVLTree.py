class avlTree:
    class Node:
        def __init__(self,key) -> None:
            self.key = key
            self.left = None #left child
            self.right = None #right child
            self.frequency = 1 # the number of the appearance of number key
            self.weight = 1 # the number of nodes in that subtree(including itself)
            self.height = 1 # height of the node

    def  __init__(self):
        self.root = None

    def left_child_condition(self, node: Node):
        if node == None:
            left_height = 0
            left_weight = 0

        elif node.left == None:
            left_height = 0
            left_weight = 0

        else:
            left_height = node.left.height
            left_weight = node.left.weight

        return left_height,left_weight

    def right_child_condition(self, node: Node):
        if node.right == None:
            right_height = 0
            right_weight = 0

        else:
            right_height = node.right.height
            right_weight = node.right.weight

        return right_height,right_weight
    
    def update(self, node: Node):
        left_height,left_weight = self.left_child_condition(node)
        right_height,right_weight = self.right_child_condition(node)



        node.height = max(left_height, right_height) + 1
        node.weight = left_weight + right_weight + node.frequency

    def adjustLeft(self,root: Node) -> Node: # when left is higher, adjust to right
        new_root = root.left
        root.left = new_root.right
        new_root.right = root
        self.update(root)
        self.update(new_root)
        return new_root

    def adjustRight(self, root: Node) -> Node:
        new_root = root.right
        root.right = new_root.left
        new_root.left = root
        self.update(root)
        self.update(new_root)
        return new_root

    def adjustBalance(self, root: Node) -> Node:
        left_height,_ = self.left_child_condition(root)
        right_height,_ = self.right_child_condition(root)
        if left_height-right_height < -1: # left lower than right
            left_height_c,_ = self.left_child_condition(root.right)
            right_height_c,_ = self.right_child_condition(root.right)
            if left_height_c - right_height_c <= 0:
                return self.adjustRight(root)
            else:
                root.right = self.adjustLeft(root.right)
                return self.adjustRight(root)
        if left_height-right_height > 1:
            left_height_c,_ = self.left_child_condition(root.left)
            right_height_c,_ = self.right_child_condition(root.left)
            if left_height_c - right_height_c >= 0:
                return self.adjustLeft(root)
            else:
                root.left = self.adjustRight(root.left)
                return self.adjustLeft(root)
            
        return root

    def insert(self, new_node_value: int, root: Node) -> Node:
        if root == None:
            return self.Node(new_node_value)
        if new_node_value == root.key:
            root.frequency += 1
        else:
            if new_node_value > root.key:
                root.right = self.insert(new_node_value,root.right)
            else:
                root.left = self.insert(new_node_value,root.left)

        self.update(root)
        return self.adjustBalance(root)
    
    def find(self,root: Node, position: int)->int:
        if root.key == self.root.key:
            if root.weight < position:
                return -1 # not find
        _,left_weight = self.left_child_condition(root)
        if left_weight < position and position <= left_weight + root.frequency:
            return root.key
        if left_weight >= position:
            return self.find(root.left,position)
        else:
            return self.find(root.right,position - left_weight - root.frequency)
    def delete(self,value: int, root: Node):
        if value > root.key:
            root.right = self.delete(value, root.right)
        elif value < root.key:
            root.left = self.delete(value, root.left)
        else: # find that value, i.e., the value is just in the node!
            if root.frequency > 1:
                root.frequency -= 1
            else:
                if root.left == None or root.right == None: # one child or no child
                    if root.left == None and root.right == None: # no child
                        return None
                    else: # only one child
                        if root.left == None: 
                            return root.right
                        else:
                            return root.left
                else: # two child
                    root.key = self.find(root.right,1) #smallest value in right subtree
                    root.right = self.delete(root.key,root.right)
        self.update(root)
        return self.adjustBalance(root)
    
    
    
    def get_position(self, root: Node, value:int,front_pos:int, equal: bool):
        if root.key == value:
            _,left_weight = self.left_child_condition(root)
            if equal == True:
                
                return front_pos + left_weight + root.frequency
            else:
                return front_pos + left_weight
        elif root.key > value:
            if root.left == None:
                return front_pos
            else:
                return self.get_position(root.left,value,front_pos,equal)
        else: # root.key < value
            _,left_weight = self.left_child_condition(root)
            if root.right == None:
                return front_pos + root.frequency
            else:
                return front_pos + self.get_position(root.right,value,left_weight+root.frequency,equal)

    def tree_insert(self,new_node_value:int):
        self.root = self.insert(new_node_value,self.root)
        return
    
    def tree_delete(self,delete_node_value:int):
        self.root = self.delete(delete_node_value,self.root)
        return
    
    def tree_get_position(self,value:int,equal:bool):
        return self.get_position(self.root,value,0,equal)
    
    def tree_find(self,position:int):
        return self.find(self.root,position)

    def tree_weight(self):
        if self.root == None:
            return 0
        else:
            return self.root.weight
    
    def get_condition_number(self,condition:list):
        if len(condition) != 3:
            return 10000000
        op = condition[1]
        print("here",op)
        # TODO: need to think twice
        try:
            val = int(condition[2])
        except:
            val = condition[2]
        if op == "=":
            condition_num = self.tree_get_position(val,True) - self.tree_get_position(val,False)
            return condition_num
        if op == ">=":
            condition_num = self.tree_weight() - self.tree_get_position(val,False)
            return condition_num
        if op == ">":
            condition_num = self.tree_weight() - self.tree_get_position(val,True)
            return condition_num
        if op == "<":
            condition_num = self.tree_get_position(val,False)
            return condition_num
        if op == "<=":
            condition_num = self.tree_get_position(val,True)
            return condition_num

if __name__ == "__main__":
    test_tree = avlTree()
    import random

    random_numbers = []

    for i in range(1,100001):

        test_tree.tree_insert(i)
    
    # print(test_tree.root.key)
    # print(test_tree.root.left.weight)
    # print(test_tree.root.right.weight)
    
    # random_numbers = random_numbers[25:]
    # random_numbers.sort()

    # print(random_numbers[40])
    # print(test_tree.tree_find(41))
    # print(random_numbers[:20])
    # print(test_tree.get_condition_number(["test","<",99990]))
    print(test_tree.tree_get_position(99990,False))



                        
        

    

            
        

        

        