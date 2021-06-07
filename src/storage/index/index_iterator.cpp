/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(int index_in_leaf, Page *page, BufferPoolManager *buffer_pool_manager)
    : index_in_leaf_(index_in_leaf), buffer_pool_manager_(buffer_pool_manager) {
  if (page != nullptr) {
    leaf_page_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
    page_id = leaf_page_->GetPageId();
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  // maybe i should judge if the page_id is INVALID?
  if (buffer_pool_manager_ != nullptr && leaf_page_ != nullptr) {
    reinterpret_cast<Page *>(leaf_page_)->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_id, false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() { return index_in_leaf_ == -1 && page_id == -1; }

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() { return leaf_page_->GetItem(index_in_leaf_); }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if (isEnd()) {
    return *this;
  }
  index_in_leaf_++;
  // ++后超出此页
  if (index_in_leaf_ == leaf_page_->GetSize()) {
    page_id_t next_page_id = leaf_page_->GetNextPageId();
    // 若下一页是invalid
    if (next_page_id == INVALID_PAGE_ID) {
      reinterpret_cast<Page *>(leaf_page_)->RUnlatch();
      buffer_pool_manager_->UnpinPage(page_id, false);
      leaf_page_ = nullptr;
      index_in_leaf_ = -1;
      page_id = -1;
      return *this;
    }
    reinterpret_cast<Page *>(leaf_page_)->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_id, false);
    Page *next_page = buffer_pool_manager_->FetchPage(next_page_id);
    next_page->RLatch();
    leaf_page_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(next_page->GetData());
    index_in_leaf_ = 0;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
