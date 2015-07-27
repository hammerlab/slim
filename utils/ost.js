// Ordered-Statistic Tree: binary search tree that supports fetching elements by index.
// Based on tree implmentations found in these gists:
//   https://gist.github.com/davidfurlong/c76a3e265ea63c1a7ba6
//   https://gist.github.com/trevmex/821973

var argv = require('minimist')(process.argv.slice(2));
var assert = require('assert');

var log = require("../utils/log").l;

var Node = function(key, value, parent) {
  this.key = key;
  this.value = value;
  this.parent = parent;
  this.leftChild = null;
  this.rightChild = null;
  this.size = 1;
  this.count = 1;
};

var n = 0;
// Use: var tree = new OST(); tree.select(4); tree.insert(key,value)
var OST = function () {
  var root = null;

  /*
   * Private Method: searchNode
   *
   * Search through a binary tree.
   *
   * Parameters:
   *     node - the node to search on.
   *     key - the key to search for (as an integer).
   *
   * Returns:
   *     the value of the found node,
   *     or null if no node was found.
   *
   */
  var searchNode = function (node, key) {
    if (node === null) {
      return null; // key not found
    }

    var nodeKey = parseInt(node.key, 10);

    if (key < nodeKey) {
      return searchNode(node.leftChild, key);
    } else if (key > nodeKey) {
      return searchNode(node.rightChild, key);
    }
    else { // key is equal to node key
      return node;
    }
  };

  var deleteNode = function(key, current) {
    current.count--;
    for (var n = current; n != null; n = n.parent) {
      n.size--;
    }
    if (current.count > 0) {
      return true;
    }
    childCount = (current.leftChild !== null ? 1 : 0) + (current.rightChild !== null ? 1 : 0);
    var parent = current.parent;
    var isLeftChild = parent && current.key < parent.key;
    var isRightChild = parent && current.key > parent.key;

    switch (childCount) {
      case 0:
        if (parent) {
          if (isLeftChild) parent.leftChild = null;
          else if (isRightChild) parent.rightChild = null;
        } else {
          root = null;
        }
        break;
      case 1:
        var child = current.leftChild || current.rightChild;
        if (parent) {
          if (isLeftChild) {
            parent.leftChild = child;
          }
          else if (isRightChild) {
            parent.rightChild = child;
          }
          child.parent = parent;
        } else {
          root = child;
          root.parent = null;
        }
        break;
      case 2:
        var replacement = current.leftChild;
        var replacementParent = current;
        for (; replacement.rightChild; replacementParent = replacement, replacement = replacement.rightChild) {}

        if (replacementParent !== current) {
          replacementParent.rightChild = replacement.leftChild;
          if (replacementParent.rightChild) {
            replacementParent.rightChild.parent = replacementParent;
          }

          for (var n = replacementParent; n !== current; n = n.parent) {
            n.size -= replacement.count;
          }

          replacement.leftChild = current.leftChild;
          if (replacement.leftChild) replacement.leftChild.parent = replacement;

        }
        replacement.rightChild = current.rightChild;
        if (replacement.rightChild) replacement.rightChild.parent = replacement;

        replacement.size =
              (replacement.leftChild ? replacement.leftChild.size : 0) +
              replacement.count +
              (replacement.rightChild ? replacement.rightChild.size : 0);

        replacement.parent = parent;
        if (parent) {
          if (isLeftChild) {
            parent.leftChild = replacement;
          } else if (isRightChild) {
            parent.rightChild = replacement;
          }
        } else {
          root = replacement;
        }
    }
    return true;
  };

  /*
   * Private Method: insertNode
   *
   * Insert into a binary tree.
   *
   * Parameters:
   *     node - the node to search on.
   *     key - the key to insert (as an integer).
   *     value - the value to associate with the key (any type of
   *             object).
   *
   * Returns:
   *     true.
   *
   */
  var insertNode = function (node, key, value, parent, depth) {
    if (node === null) {
      node = new Node(key, value, parent);
      if (parent) {
        if (key < parent.key) parent.leftChild = node;
        else parent.rightChild = node;
      }
      if (!root) {
        root = node;
      }
      var recurseToRoot = node;
      while(recurseToRoot.parent != null){
        recurseToRoot.parent.size++;
        recurseToRoot = recurseToRoot.parent;
      }
      return true;
    }

    var nodeKey = parseInt(node.key, 10);

    if (key < nodeKey) {
      insertNode(node.leftChild, key, value, node, depth + 1);
    }
    else if (key > nodeKey) {
      insertNode(node.rightChild, key, value, node, depth + 1);
    }
    else { // key is equal to node key, increment the count
      node.count++;
      while (true) {
        if (!node) break;
        node.size++;
        node = node.parent;
      }
    }
    return true;
  };

  /*
   * Private Method: traverseNode
   *
   * Call a function on each node of a binary tree.
   *
   * Parameters:
   *     node - the node to traverse.
   *     callback - the function to call on each node, this function
   *                takes a key and a value as parameters.
   *
   * Returns:
   *     true.
   *
   */
  var traverseNode = function (node, callback) {
    if (node !== null) {
      traverseNode(node.leftChild, callback);
      callback(node, node.key, node.value, node.count, node.size);
      traverseNode(node.rightChild, callback);
    }

    return true;
  };

  this.str = function(delim) {
    var arr = [];
    var nodes = root ? [root] : [];
    var seenKeys = {};
    while (nodes.length) {
      var cur = nodes.pop();
      if (cur.key in seenKeys) continue;
      if (cur.leftChild) {
        nodes.push(cur.leftChild);
      }
      if (cur.rightChild) {
        nodes.push(cur.rightChild);
      }
      seenKeys[cur.key] = true;
      var s = [
        cur.value,
        "(",
        cur.count,
        ",",
        cur.size,
        "): L: ",
        (cur.leftChild && (cur.leftChild.value + "(" + cur.leftChild.count + "," + cur.leftChild.size + ")") || ""),
        ", R: ",
        (cur.rightChild && (cur.rightChild.value + "(" + cur.rightChild.count + "," + cur.rightChild.size + ")") || "")
      ].join('');
      arr.push(s);
    }
    return "**" + this.size() + "**:\n\t" + arr.join(delim || "\n\t");
  };

  /*
   * Private Method: minNode
   *
   * Find the key of the node with the lowest key number.
   *
   * Parameters:
   *     node - the node to traverse.
   *
   * Returns: the key of the node with the lowest key number.
   *
   */
  var minNode = function (node) {
    while (node.leftChild !== null) {
      node = node.leftChild;
    }

    return node;
  };

  /*
   * Private Method: maxNode
   *
   * Find the key of the node with the highest key number.
   *
   * Parameters:
   *     node - the node to traverse.
   *
   * Returns: the key of the node with the highest key number.
   *
   */
  var maxNode = function (node) {
    while (node.rightChild !== null) {
      node = node.rightChild;
    }

    return node;
  };

  /*
   * Private Method: successorNode
   *
   * Find the key that successes the given node.
   *
   * Parameters:
   *		node - the node to find the successor for
   *
   * Returns: the node that successes the given node.
   *
   */
  var successorNode = function (node) {
    var parent;

    if (node.rightChild !== null) {
      return minNode(node.rightChild);
    }

    parent = node.parent;
    while (parent !== null && node == parent.rightChild) {
      node = parent;
      parent = parent.parent;
    }

    return parent
  };

  var self = this;
  var syncCheckNode = function(node, depth) {
    if (!node) {
      return;
    }
    var l = node.leftChild && node.leftChild.size || 0;
    var r = node.rightChild && node.rightChild.size || 0;
    assert.equal(node.size, l + r + node.count, node.size + " != " + l + " + " + node.count + " + " + r + ":\n" + self.str("\n\t"));
    assert(node.size, self.str("\n\t"));
    assert(node.count, self.str("\n\t"));
    if (node.leftChild) {
      assert.equal(node.key, node.leftChild.parent.key, self.str("\n\t"));
      assert(node.leftChild.key < node.key, self.str("\n\t"));
    }
    if (node.rightChild) {
      assert.equal(node.key, node.rightChild.parent.key, self.str("\n\t"));
      assert(node.rightChild.key > node.key, self.str("\n\t"));
    }
    syncCheckNode(node.leftChild, depth + 1);
    syncCheckNode(node.rightChild, depth + 1);
  };

  /*
   * Private Method: selectNode
   *
   * find the i'th smallest element stored in the tree
   *
   * Parameters:
   *      node - initially root
   *      i - index
   *
   * Returns: find the i'th smallest element stored in the tree
   *
   */
  var selectNode = function (i, node) {
    if (!node) {
      log.error("selecting %d at null node :(", i);
      return null;
    }
    var l = node.leftChild && node.leftChild.size || 0;
    var r = node.rightChild && node.rightChild.size || 0;
    //console.log("at %d, selecting idx %d. counts: %d/%d/%d (%d - %d = %d)", node.value, i, l, node.count, r, node.size, l + node.count + r, node.size - l - node.count - r);
    if (i < l) {
      //console.log("\tgoing left");
      return selectNode(i, node.leftChild);
    } else if (l + node.count <= i) {
      if (i == 0 && node.count == 0) {
        return node;
      }
      //console.log("\tgoing right");
      return selectNode(i - (l + node.count), node.rightChild);
    } else {
      return node;
    }
  };
  /*
   * Private Method: rankNode
   *
   * find the rank of node x in the tree, i.e. its index in the sorted list of elements of the tree
   *
   * Parameters:
   *      x - Node
   *
   * Returns: the rank of x
   *
   */
  var rankNode = function (x) {
    var l = x.leftChild.size + 1;
    var iter = x;
    var r = 1;
    while(iter != root){
      if(iter == iter.parent.rightChild)
        r = r + iter.parent.leftChild.size + 1;
      iter = iter.parent;
    }
    return r;
  };

  /*
   * Private Method: predecessorNode
   *
   * Find the key that preceeds the given node.
   *
   * Parameters:
   *		node - the node to find the predecessor for
   *
   * Returns: the node that preceeds the given node.
   *
   */
  var predecessorNode = function (node) {
    var parent;

    if (node.leftChild !== null) {
      return maxNode(node.leftChild);
    }

    parent = node.parent;
    while (parent !== null && node == parent.leftChild) {
      node = parent;
      parent = parent.parent;
    }

    return parent;
  };

    /*
     * Method: search
     *
     * Search through a binary tree.
     *
     * Parameters:
     *     key - the key to search for.
     *
     * Returns:
     *     the node,
     *     or null if no node was found,
     *     or undefined if no key was specified.
     *
     */
  this.search = function (key) {
    var keyInt = parseInt(key, 10);

    if (isNaN(keyInt)) {
      return undefined; // key must be a number
    }
    else {
      return searchNode(root, keyInt);
    }
  };
  /*
     * Method: select
     *
     * find the i'th smallest element stored in the tree
     *
     * Parameters:
     *     i - index
     *
     *
     * Returns:
     *     the node,
     *     or undefined if i > size of tree
     *
     */
  this.select = function (i) {
    if(!root || root.size < i)
      return undefined;
    else
      return selectNode(i, root);
  };
  this.size = function() {
    return root && root.size || 0;
  };
  /*
     * Method: rank
     *
     * find the rank of element x in the tree, i.e. its index in the sorted list of elements of the tree
     *
     *
     * Parameters:
     *     x - element
     *
     *
     * Returns:
     *     rank of x (one-indexed!!!! i.e. 1,2,3...)
     *     or undefined if x is undefined
     *
     */
  this.rank = function (x) {
    return rankNode(x);
  };

  /*
     * Method: insert
     *
     * Insert into a binary tree.
     *
     * Parameters:
     *     key - the key to search for.
     *     value - the value to associate with the key (any type of
     *             object).
     *
     * Returns:
     *     true,
     *     or undefined if no key was specified.
     *
     */
  this.insert = function (key, value) {
    var keyInt = parseInt(key, 10);

    if (isNaN(keyInt)) {
      log.error("bad key:", key);
      return undefined; // key must be a number
    }
    else {
      return insertNode(root, keyInt, value, null, 0);
    }
  };

  /*
     * Method: delete
     *
     * Deletes Node x
     *
     * Parameters:
     *     x - the Node to delete (of type Node)
     *
     * Returns:
     *     true.
     *
     */
  this.delete = function (x) {
    return deleteNode(x.key, x);
  };
    /*
     * Method: traverse
     *
     * Call a function on each node of a binary tree.
     *
     * Parameters:
     *     callback - the function to call on each node, this function
     *                takes a key and a value as parameters. If no
     *                callback is specified, print is called.
     *
     * Returns:
     *     true.
     *
     */
  this.traverse = function (callback) {
      if (typeof callback === "undefined") {
        callback = function (key, value) {
          print(key + ": " + value);
        };
      }

      return traverseNode(root, callback);
    };
  this.array = function() {
    var arr = [];
    this.traverse(function(n, k, v, c, s) {
      arr.push([k,c]);
    });
    return arr;
  };
  this.arrayStr = function() {
    return JSON.stringify(this.array());
  };

  /*
     * Method: min
     *
     * Find the key of the node with the lowest key number.
     *
     * Parameters: none
     *
     * Returns: the node with the lowest key number.
     *
     */
  this.min = function () {
      return minNode(root);
  };

    /*
     * Method: max
     *
     * Find the key of the node with the highest key number.
     *
     * Parameters: none
     *
     * Returns: the node with the highest key number.
     *
     */
  this.max = function () {
      return maxNode(root);
  };
  // Returns the root node
  this.getRoot = function() {
    return root;
  };
  /*
     * Method: successor
     *
     * Find the key that successes the root node.
     *
     * Parameters: none
     *
     * Returns: the node that successes the root node.
     *
     */
  this.successor = function () {
    return successorNode(root);
  };
  /*
     * Method: predecessor
     *
     * Find the key that preceeds the root node.
     *
     * Parameters: none
     *
     * Returns: the node that preceeds the root node.
     *
     */
  this.predecessor = function () {
    return predecessorNode(root);
  };

  this.syncCheck = function() {
    assert.equal(null, root.parent);
    syncCheckNode(root, 0);
  };

};

module.exports.OST = OST;

/* David Furlong used the below BST Class to build the OST Class
 *
 * License:
 *
 * Copyright (c) 2011 Trevor Lalish-Menagh (http://www.trevmex.com/)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
