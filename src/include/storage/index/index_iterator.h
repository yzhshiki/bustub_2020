//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include <list>
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(int index_in_leaf, Page *page, BufferPoolManager *buffer_pool_manager);
  ~IndexIterator();

  bool isEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

  bool operator==(const IndexIterator &itr) const {
    return index_in_leaf_ == itr.index_in_leaf_ && page_id == itr.page_id;
  }

  bool operator!=(const IndexIterator &itr) const { return !(*this == itr); }

 private:
  // add your own private member variables here
  page_id_t page_id{INVALID_PAGE_ID};
  int index_in_leaf_{-1};
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page_{nullptr};
  BufferPoolManager *buffer_pool_manager_{nullptr};
};

}  // namespace bustub
