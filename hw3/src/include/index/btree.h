#pragma once

#include <cstddef>
#include <cstring>
#include <functional>
#include <iostream>
#include <optional>

#include "buffer/buffer_manager.h"
#include "common/defer.h"
#include "common/macros.h"
#include "storage/segment.h"

#define UNUSED(p)  ((void)(p))

namespace buzzdb {

template<typename KeyT, typename ValueT, typename ComparatorT, size_t PageSize>
struct BTree : public Segment {
    struct Node {

        /// The level in the tree.
        uint16_t level;

        /// The number of children.
        uint16_t count;

        // Constructor
        Node(uint16_t level, uint16_t count)
            : level(level), count(count) {}

        /// Is the node a leaf node?
        bool is_leaf() const { return level == 0; }

        std::optional<uint64_t> parentPageId;
    };

    struct InnerNode: public Node {
        /// The capacity of a node.
        /// TODO think about the capacity that the nodes have.
        static constexpr uint32_t kCapacity = (PageSize - sizeof(Node)) / (sizeof(KeyT) + sizeof(ValueT));;

        /// The keys.
        KeyT keys[kCapacity];

        /// The children.
        uint64_t children[kCapacity];

        /// Constructor.
        InnerNode() : Node(0, 0) {}

        /// Get the index of the first key that is not less than than a provided key.
        /// @param[in] key          The key that should be searched.
        std::pair<uint32_t, bool> lower_bound(const KeyT &key) {
	      // TODO: remove the below lines of code 
    	      // and add your implementation here
            int low = 0;
            int high = this->count - 1;
            const ComparatorT comparator = ComparatorT();
            if (!comparator(this->keys[0], key) && high == 1) {
                return {0, true};
            }
            std::optional<uint32_t> index;
            while (low <= high) {
                if (key > this->keys[low + (high - low) / 2]) {
                    low = (low + (high - low) / 2) + 1;
                } else if (key < this->keys[low + (high - low) / 2]) {
                    index = (low + (high - low) / 2);
                    high = (low + (high - low) / 2) - 1;
                } else {
                    return {static_cast<uint32_t>(low + (high - low) / 2), true};
                }
            }
            if (!index) {
                return {0, false};
            }
            return {*index, true};
        }

        /// Insert a key.
        /// @param[in] key          The separator that should be inserted.
        /// @param[in] split_page   The id of the split page that should be inserted.
        void insert(const KeyT &key, uint64_t split_page) {
 	      // TODO: remove the below lines of code 
    	      // and add your implementation here
	    //   UNUSED(key);
	    //   UNUSED(split_page);
            std::optional<int> index;
            std::vector<uint64_t> tempChildren;
            std::vector<KeyT> tempKeys;
            int i = 0;
            int lastElement = this->count - 1;
            while(i < lastElement) {
                tempChildren.push_back(this->children[i]);
                tempKeys.push_back(this->keys[i]);
                const ComparatorT comparator = ComparatorT();
                if(!(index || comparator(this->keys[i], key))) {
                    index = i;
                }
                this->children[i] = 0;
                this->keys[i] = 0;
                i++;
            }
            tempChildren.push_back(this->children[lastElement]);
            this->children[lastElement] = 0;
            if (index) {
                tempKeys.insert(tempKeys.begin() + *index, key);
                index = *index + 1;
                tempChildren.insert(tempChildren.begin() + *index, split_page);
            } else {
                tempKeys.insert(tempKeys.begin() + (this->count - 1), key);
                tempChildren.insert(tempChildren.begin() + this->count, split_page);
            }
            this->count++;
            i = 0;
            while (i < static_cast<int>(tempChildren.size())) {
                this->children[i] = tempChildren[i];
                i++;
            }
            i = 0;
            while (i < static_cast<int>(tempKeys.size())) {
                this->keys[i] = tempKeys[i];
                i++;
            }
        }

        /// Split the node.
        /// @param[in] buffer       The buffer for the new page.
        /// @return                 The separator key.
        KeyT split(std::byte* buffer) {
            // TODO: remove the below lines of code 
    	      // and add your implementation here
		//UNUSED(buffer);
            auto newInnerNode = reinterpret_cast<InnerNode*>(buffer);
            int startIndex = 0;
            auto separatorKey = static_cast<int>((this->keys[((this->kCapacity - 1) / 2) + 1] + this->keys[((this->kCapacity - 1) / 2)]) / 2);
            int i = ((this->kCapacity - 1) / 2) + 1;
            while (i < static_cast<int>(this->kCapacity) + 1) {
                if (startIndex != 0) {
                    newInnerNode->keys[startIndex - 1] = this->keys[i - 1];
                    this->keys[i - 1] = 0;
                } else {
                    newInnerNode->keys[startIndex] = this->keys[i];
                    newInnerNode->children[startIndex] = this->children[i];
                    this->keys[i] = 0;
                    this->children[i] = 0;
                    this->count--;
                    startIndex++;
                    i++;
                }
                newInnerNode->children[startIndex] = this->children[i];
                this->children[i] = 0;
                this->count--;
                startIndex++;
                i++;
            }
            newInnerNode->count = startIndex;
            return separatorKey;
        }

        /// Returns the keys.
        /// Can be implemented inefficiently as it's only used in the tests.
        std::vector<KeyT> get_key_vector() {
            // TODO
            return {keys.begin(), keys.begin() + (this->count - 1)}; 
        }

        /// Returns the child page ids.
        /// Can be implemented inefficiently as it's only used in the tests.
        std::vector<uint64_t> get_child_vector() {
            // TODO
	    //return std::vector<uint64_t>();
            return {children.begin(), children.begin() + this->count}; 
        }
    };

    struct LeafNode: public Node {
        /// The capacity of a node.
        /// TODO think about the capacity that the nodes have.
        static constexpr uint32_t kCapacity = (PageSize - sizeof(Node)) / (sizeof(KeyT) + sizeof(ValueT));

        /// The keys.
        KeyT keys[kCapacity];

        /// The values.
        ValueT values[kCapacity];

        /// Constructor.
        LeafNode() : Node(0, 0) {}

        std::pair<uint32_t, bool> lower_bound(const KeyT& key) {
            if (this->count == 0) {
                return {0, false}; 
            }
            uint32_t first = 0;
            uint32_t cnt = this->count;
            while (cnt > 0) {
                uint32_t idx = first;
                idx += cnt / 2;
                const ComparatorT comparator = ComparatorT();
                if (!comparator(keys[idx], key)) {
                    cnt /= 2;
                } else {
                    first = ++idx;
                    cnt -= cnt / 2 + 1;
                }
            }
            return {first, (keys[first] == key && first < this->count)};
        }
        /// Insert a key.
        /// @param[in] key          The key that should be inserted.
        /// @param[in] value        The value that should be inserted.
        void insert(const KeyT &key, const ValueT &value) {
            //TODO
            // UNUSED(key);
            // UNUSED(value);
            if (this->count == 0) {
                keys[0] = key;
                values[0] = value;
            } else {
                if (lower_bound(key).second) {
                    values[lower_bound(key).first] = value;
                    return;
                }
                std::memmove(&keys[lower_bound(key).first + 1], &keys[lower_bound(key).first], sizeof(KeyT) * (this->count - lower_bound(key).first));
                std::memmove(&values[lower_bound(key).first + 1], &values[lower_bound(key).first], sizeof(ValueT) * (this->count - lower_bound(key).first));
                keys[lower_bound(key).first] = key;
                values[lower_bound(key).first] = value;
            }
            this->count++;
        }

        /// Erase a key.
        void erase(const KeyT &key) {
            //TODO
            if (this->count == 0) {
                return; 
            }
            if (!lower_bound(key).second) {
                return;
            }
            const uint32_t num_to_copy = (this->count - 1) - lower_bound(key).first;
            std::memmove(&values[lower_bound(key).first], &values[lower_bound(key).first + 1], sizeof(ValueT) * num_to_copy);
            std::memmove(&keys[lower_bound(key).first], &keys[lower_bound(key).first + 1], sizeof(KeyT) * num_to_copy);
            this->count--;
        }

        /// Split the node.
        /// @param[in] buffer       The buffer for the new page.
        /// @return                 The separator key.
        KeyT split(std::byte* buffer) {
            // TODO
            int startIndex = 0;
            int i = ((this->kCapacity - 1) / 2) + 1;
            while (i < static_cast<int>(this->kCapacity)) {
                reinterpret_cast<LeafNode*>(buffer)->keys[startIndex] = this->keys[i];
                reinterpret_cast<LeafNode*>(buffer)->values[startIndex] = this->values[i];
                startIndex++;
                this->count--;
                this->keys[i] = 0;
                this->values[i] = 0;
                i++;
            }
            reinterpret_cast<LeafNode*>(buffer)->count = startIndex;
            return this->keys[startIndex - 1];
        }

        /// Returns the keys.
        /// Can be implemented inefficiently as it's only used in the tests.
        std::vector<KeyT> get_key_vector() {
            // TODO
            return {keys.begin(), keys.begin() + this->count};
        }

        /// Returns the values.
        /// Can be implemented inefficiently as it's only used in the tests.
        std::vector<ValueT> get_value_vector() {
            // TODO
            return {values.begin(), values.begin() + this->count};
        }
    };

    /// The root.
    std::optional<uint64_t> root;
    uint16_t rootLevel = 0;
    bool isTreeEmpty;
    std::map<KeyT, bool> deletedKeys;

    /// Next page id.
    /// You don't need to worry about about the page allocation.
    /// Just increment the next_page_id whenever you need a new page.
    uint64_t next_page_id;

    /// Constructor.
    BTree(uint16_t segment_id, BufferManager &buffer_manager)
        : Segment(segment_id, buffer_manager) {
        // TODO
        //next_page_id = 1;
        this->isTreeEmpty = true;
    }

    /// Lookup an entry in the tree.
    /// @param[in] key      The key that should be searched.
    std::optional<ValueT> lookup(const KeyT &key) {
        // TODO
	// UNUSED(key);
	// return std::optional<ValueT>();
        bool found = false;
        std::optional<ValueT> foundKey;
        if (!this->root) {
            return foundKey;
        }
        if (this->deletedKeys.find(key) != this->deletedKeys.end()) {
            return foundKey;
        }
        auto currentPageId = *this->root;
        int next = 0;
        uint64_t previousParentPageId = currentPageId;
        while (!found) {
            auto& currentPage = this->buffer_manager.fix_page(currentPageId, false);
            auto currentNode = reinterpret_cast<Node*>(currentPage.get_data());
            if (!currentNode->is_leaf()) {
                auto innerNode = reinterpret_cast<InnerNode*>(currentNode);
                if (currentNode->parentPageId) {
                    previousParentPageId = *currentNode->parentPageId;
                }
                if (!innerNode->lower_bound(key).second) {
                    currentPageId = innerNode->children[innerNode->count - 1];
                } else {
                    if (innerNode->count >= next) {
                        currentPageId = innerNode->children[innerNode->lower_bound(key).first + next];
                        next = 0;
                    } else {
                        return foundKey;
                    }
                }
            } else {
                int i = 0;
                while (i < reinterpret_cast<LeafNode*>(currentNode)->count) {
                    if (reinterpret_cast<LeafNode*>(currentNode)->keys[i] == key) {
                        foundKey = reinterpret_cast<LeafNode*>(currentNode)->values[i];
                        return foundKey;
                    }
                    i++;
                }
                if (next == 2) {
                    return foundKey;
                }
                currentPageId = previousParentPageId;
                next++;
            }
            this->buffer_manager.unfix_page(currentPage, false);
        }
        return foundKey;
    }


    /// Erase an entry in the tree.
    /// @param[in] key      The key that should be searched.
    void erase(const KeyT &key) {
        // TODO
	//UNUSED(key);
        if (this->root) {
            bool found = false;
            auto currentPageId = *this->root;
            int next = 0;
            uint64_t previousParentPageId = currentPageId;
            while (!found) {
                auto& currentPage = this->buffer_manager.fix_page(currentPageId, true);
                auto currentNode = reinterpret_cast<Node*>(currentPage.get_data());
                if (!currentNode->is_leaf()) {
                    auto innerNode = reinterpret_cast<InnerNode*>(currentNode);
                    if (currentNode->parentPageId) {
                        previousParentPageId = *currentNode->parentPageId;
                    }
                    if (!innerNode->lower_bound(key).second) {
                        currentPageId = innerNode->children[innerNode->count - 1];
                    } else {
                        if (innerNode->count >= next) {
                            currentPageId = innerNode->children[innerNode->lower_bound(key).first + next];
                            next = 0;
                        } else {
                            found = true;
                        }
                    }
                } else {
                    auto leafNode = reinterpret_cast<LeafNode*>(currentNode);
                    int i = 0;
                    while (i < leafNode->count) {
                        if (leafNode->keys[i] == key) {
                            found = true;
                            leafNode->erase(key);
                            this->deletedKeys.insert(std::pair<KeyT,bool>(key,true));
                            break;
                        }
                        i++;
                    }
                    currentPageId = previousParentPageId;
                    next++;
                }
                this->buffer_manager.unfix_page(currentPage, true);
            }
        }
    }

    /// Inserts a new entry into the tree.
    /// @param[in] key      The key that should be inserted.
    /// @param[in] value    The value that should be inserted.
    void insert(const KeyT &key, const ValueT &value) {
        // TODO
	    // new root
	    if (this->isTreeEmpty) {
            this->isTreeEmpty = false;
            this->next_page_id = 1;
            this->root = 0;
        }
        auto currentNodePageId = *this->root;
        bool KeyInserted = false;
        while (!KeyInserted) {
            auto& currentPage = this->buffer_manager.fix_page(currentNodePageId, true);
            auto currentNode = reinterpret_cast<Node*>(currentPage.get_data());
            if (!currentNode->is_leaf()) {
                    auto innerNode = static_cast<InnerNode*>(currentNode);
                /// create new inner node if current inner node is full
                if (innerNode->count == (innerNode->kCapacity + 1)) {
                    /// create new inner node
                    auto newInnerNodePageId = this->next_page_id;
                    this->next_page_id++;
                    auto& newInnerNodePage = this->buffer_manager.fix_page(newInnerNodePageId, true);
                    KeyT separatorKey = innerNode->split(reinterpret_cast<std::byte *>(newInnerNodePage.get_data()));
                    auto newNode = reinterpret_cast<Node*>(newInnerNodePage.get_data());
                    auto newInnerNode = static_cast<InnerNode*>(newNode);
                    newInnerNode->level = innerNode->level;
                    /// set the new parent id of the children
                    int i = 0;
                    while (i < newInnerNode->count) {
                        auto& child = this->buffer_manager.fix_page(newInnerNode->children[i], true);
                        auto child_node = reinterpret_cast<Node*>(child.get_data());
                        auto child_innerNode = static_cast<InnerNode*>(child_node);
                        child_innerNode->parentPageId = newInnerNodePageId;
                        i++;
                    }
                    if (innerNode->parentPageId) {
                        /// get the parent node of the current node
                        auto& parentNodePage = this->buffer_manager.fix_page(*innerNode->parentPageId, true);
                        auto parentNode = reinterpret_cast<Node *>(parentNodePage.get_data());
                        auto parentInnerNode = static_cast<InnerNode *>(parentNode);
                        parentInnerNode->insert(separatorKey, newInnerNodePageId);
                        newInnerNode->parentPageId = *innerNode->parentPageId;
                        /// move to next node
                        if (parentInnerNode->lower_bound(key).second) {
                            /// go to rhs child
                            currentNodePageId = parentInnerNode->children[parentInnerNode->count - 1];
                        } else {
                            /// go to lhs child
                            currentNodePageId = parentInnerNode->children[parentInnerNode->lower_bound(key).first];
                        }
                        this->buffer_manager.unfix_page(parentNodePage, true);
                    } else {
                        /// root has new page id
                        this->root = this->next_page_id;
                        this->next_page_id++;
                        auto& newRootNodePage = this->buffer_manager.fix_page(*this->root, true);
                        auto newRootNode = reinterpret_cast<Node *>(newRootNodePage.get_data());
                        auto newRootInnerNode = static_cast<InnerNode *>(newRootNode);
                        rootLevel++;
                        newRootInnerNode->level = rootLevel;
                        /// add first key
                        newRootInnerNode->keys[0] = separatorKey;
                        /// add children to first key
                        newRootInnerNode->children[0] = currentNodePageId;
                        newRootInnerNode->children[1] = newInnerNodePageId;
                        /// increase children number
                        newRootInnerNode->count += 2;
                        /// set parent page id for both inner nodes
                        innerNode->parentPageId = *this->root;
                        newInnerNode->parentPageId = *this->root;
                        this->buffer_manager.unfix_page(newRootNodePage, true);
                        if (!newRootInnerNode->lower_bound(key).second) {
                            /// go to rhs child
                            currentNodePageId = newRootInnerNode->children[newRootInnerNode->count - 1];
                        } else {
                            /// go to lhs child
                            currentNodePageId = newRootInnerNode->children[newRootInnerNode->lower_bound(key).first];
                        }
                    }
                    this->buffer_manager.unfix_page(newInnerNodePage, true);
                } else {
                    /// if key is greater than any of the keys in the inner node go to last children
                    if (!innerNode->lower_bound(key).second) {
                        /// go to rhs child
                        currentNodePageId = innerNode->children[innerNode->count - 1];
                    } else {
                        /// go to lhs child
                        currentNodePageId = innerNode->children[innerNode->lower_bound(key).first];
                    }
                }
                this->buffer_manager.unfix_page(currentPage, true);
            } else {
                auto leafNode = static_cast<LeafNode*>(currentNode);
                /// check if there is space for the key
                if (leafNode->count < leafNode->kCapacity) {
                    leafNode->insert(key, value);
                    this->buffer_manager.unfix_page(currentPage, true);
                    KeyInserted = true;
                } else {
                    /// create new leaf node
                    auto newLeafPageId = this->next_page_id;
                    this->next_page_id++;
                    auto& newLeafPage = this->buffer_manager.fix_page(newLeafPageId, true);
                    KeyT separatorKey = leafNode->split(reinterpret_cast<std::byte *>(newLeafPage.get_data()));
                    auto newNode = reinterpret_cast<Node*>(newLeafPage.get_data());
                    auto newLeafNode = static_cast<LeafNode*>(newNode);
                    /// add new key if separatorKey is not lower than key add to lhs, else add to rhs
                    const ComparatorT comparator = ComparatorT();
                    if(comparator(separatorKey, key)) {
                        newLeafNode->insert(key, value);
                    } else {
                        leafNode->insert(key, value);
                    }
                    if (leafNode->parentPageId) {
                        /// add separatorKey into existing parent node
                        auto& parentNodePage = this->buffer_manager.fix_page(*leafNode->parentPageId, true);
                        auto parentNode = reinterpret_cast<Node *>(parentNodePage.get_data());
                        auto parentInnerNode = static_cast<InnerNode *>(parentNode);
                        parentInnerNode->insert(separatorKey, newLeafPageId);
                        newLeafNode->parentPageId = *leafNode->parentPageId;
                        this->buffer_manager.unfix_page(parentNodePage, true);
                    } else {
                        /// root has new page id
                        this->root = this->next_page_id;
                        this->next_page_id++;
                        auto& newRootNodePage = this->buffer_manager.fix_page(*this->root, true);
                        auto new_inner_root_node = reinterpret_cast<Node *>(newRootNodePage.get_data());
                        auto newRootNode = static_cast<InnerNode *>(new_inner_root_node);
                        rootLevel++;
                        newRootNode->level = rootLevel;
                        /// add first key
                        newRootNode->keys[0] = separatorKey;
                        /// add children to first key
                        newRootNode->children[0] = currentNodePageId;
                        newRootNode->children[1] = newLeafPageId;
                        /// increase children number
                        newRootNode->count += 2;
                        /// set parent page id for both leaf nodes
                        leafNode->parentPageId = *this->root;
                        newLeafNode->parentPageId = *this->root;
                        this->buffer_manager.unfix_page(newLeafPage, true);
                        this->buffer_manager.unfix_page(newRootNodePage, true);
                    }
                    this->buffer_manager.unfix_page(currentPage, true);
                    KeyInserted = true;
                }
            }
        }
    }
};

}  // namespace buzzdb
