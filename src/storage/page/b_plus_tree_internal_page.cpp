//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  KeyType key = array[index].first;
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array[index].first = key; }

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  int size = GetSize();
  for (int i = 0; i < size; ++i) {
    if (value == array[i].second) {
      return i;
    }
  }
  return INVALID_PAGE_ID;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const { return array[index].second; }

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  // ???Ki????????????>=key???k???ki==key?????????pi+1???ki>key?????????pi???
  // ?????????????????????????????????array[0]???invalid?????????ki?????????array[i].first???pi???array[i-1].second??????ki?????????
  // ?????????key??????????????????key???????????????
  int size = GetSize();
  assert(size != 0);
  if (comparator(key, array[size - 1].first) >= 0) {
    return array[size - 1].second;
  }
  // ????????????
  int l = 1;
  int r = size - 1;
  int m = (l + r) >> 1;
  while (l < r) {
    m = (l + r + 1) >> 1;
    int result = comparator(array[m].first, key);
    if (result == -1) {
      l = m;
    } else if (result == 1) {
      r = m - 1;
    } else if (result == 0) {
      return array[m].second;
    }
  }
  if (comparator(key, array[r].first) == -1) {
    return array[r - 1].second;
  }
  return array[r].second;
  // // ????????????
  // for (int i = 1; i < size; i++) {
  //   if (comparator(key, array[i].first) == -1) {
  //     return array[i - 1].second;
  //   }
  //   if (comparator(key, array[i].first) == 0) {
  //     return array[i].second;
  //   }
  // }
  // return array[size - 1].second;
}

/* Insert helper function
 * Insert at specified index, and IncreaseSize(1)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertAt(const int index, const KeyType &new_key, const ValueType &new_value) {
  int size = GetSize();
  for (int i = size; i > index; --i) {
    array[i] = array[i - 1];
  }
  array[index] = std::make_pair(new_key, new_value);
  // std::cout<<"now size: "<<GetSize()<<std::endl;
  IncreaseSize(1);
  // std::cout<<"now size: "<<GetSize()<<std::endl;
}

// SetParentToMe, for the moved internal page's children
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetParentToMe(page_id_t page_id, BufferPoolManager *buffer_pool_manager) {
  Page *page = buffer_pool_manager->FetchPage(page_id);
  BPlusTreePage *child_page = reinterpret_cast<BPlusTreePage *>(page);
  child_page->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(page_id, true);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  array[0].second = old_value;
  array[1].first = new_key;
  array[1].second = new_value;
  SetSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  int i = ValueIndex(old_value);
  // ???????????????????????????????????????
  if (i == -1) {
    return GetSize();
  }
  InsertAt(i + 1, new_key, new_value);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  // ?????????????????????KV??????????????????T??????????????????KV????????????T?????????T?????????????????????????????????????????????????????????
  // ??????????????????????????????????????????????????????????????????T???????????????
  int size = GetSize();
  // 4??????2??????5??????2??????
  int move_size = size / 2;
  recipient->CopyNFrom(array + size - move_size, move_size, buffer_pool_manager);
  // std::cout<<"now size: "<<GetSize()<<std::endl;
  IncreaseSize(-move_size);
  // std::cout<<"now size: "<<GetSize()<<std::endl;
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  // ?????????R?????????L?????????????????????move_size???KV??????????????????????????????
  for (int i = 0; i < size; ++i) {
    CopyLastFrom(items[i], buffer_pool_manager);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  int size = GetSize();
  for (int i = index; i < size - 1; ++i) {
    array[i] = array[i + 1];
  }
  // std::cout<<"now size: "<<GetSize()<<std::endl;
  IncreaseSize(-1);
  // std::cout<<"now size: "<<GetSize()<<std::endl;
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  ValueType page_id = ValueAt(0);
  Remove(0);
  return page_id;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager, bool ToEnd) {
  // if????????????Move??????????????????Move
  if (!ToEnd) {
    recipient->SetKeyAt(0, middle_key);
    int size = GetSize();
    for (int i = 0; i < size; ++i) {
      MoveLastToFrontOf(recipient, array[i].first, buffer_pool_manager);
    }
    SetSize(0);
    return;
  }
  int recv_data_len = recipient->GetSize();
  // ???this?????????recipient????????????????????????????????????????????????middlekey+?????????this??????????????????????????????KV????????????????????????
  recipient->array[recv_data_len] = std::make_pair(middle_key, array[0].second);
  recipient->CopyNFrom(array + 1, GetSize() - 1, buffer_pool_manager);
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  recipient->CopyLastFrom(std::make_pair(middle_key, array[0].second), buffer_pool_manager);
  Remove(0);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  InsertAt(GetSize(), pair.first, pair.second);
  SetParentToMe(pair.second, buffer_pool_manager);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient???s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  recipient->CopyFirstFrom(std::make_pair(middle_key, array[size - 1].second), buffer_pool_manager);
  Remove(size - 1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 * ?????????????????????array[1]???key???array[0]???value
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  SetParentToMe(pair.second, buffer_pool_manager);
  InsertAt(0, pair.first, pair.second);
  array[1].first = pair.first;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
