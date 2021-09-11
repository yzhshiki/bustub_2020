//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  if (IsEmpty()) {
    return false;
  }
  Page *page = FindLeafPage(key, false, Operation::READ, transaction);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  ValueType value{};
  bool exist;
  exist = leaf_page->Lookup(key, &value, comparator_);
  if (exist) {
    result->push_back(value);
  }
  if (transaction != nullptr) {
    UnpinAndUnLatch(Operation::READ, transaction);
  } else {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  }
  return exist;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  // if(transaction != nullptr) {std::cout<<transaction->GetThreadId()<<" in Insert "<<std::endl;}
  root_latch_.lock();
  if (IsEmpty()) {
    StartNewTree(key, value);
    root_latch_.unlock();
    return true;
  }
  root_latch_.unlock();
  bool ans = InsertIntoLeaf(key, value, transaction, false);
  UnpinAndUnLatch(Operation::INSERT, transaction);
  // if(transaction != nullptr) {std::cout<<transaction->GetThreadId()<<" out Insert "<<std::endl;}
  return ans;
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  Page *page = buffer_pool_manager_->NewPage(&root_page_id_);
  // std::cout<<"up startnewtree\n";
  page->WLatch();
  // std::cout<<"down startnewtree\n";
  if (page == nullptr && IsEmpty()) {
    page->WUnlatch();
    throw Exception(ExceptionType::OUT_OF_MEMORY, "out of memory when start new tree");
  }
  root_page_id_ = page->GetPageId();
  LeafPage *root_page = reinterpret_cast<LeafPage *>(page->GetData());
  root_page->Init(root_page_id_, root_page_id_, leaf_max_size_);
  root_page->Insert(key, value, comparator_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
  UpdateRootPageId(0);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 * 进入后获得此页写锁，split中获得sibling的写锁，insertInParent中释放parent与sibling的写锁
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction,
                                    bool notfull) {
  // FindLeafPageInTran(key, false, transaction, notfull);
  Page *leaf_page = FindLeafPage(key, false, Operation::INSERT, transaction);
  assert(leaf_page != nullptr);
  LeafPage *leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  if (leaf->GetPageId() == -1 || leaf->GetParentPageId() == -1) {
    std::cout << "warning\n";
  }
  int old_size = leaf->GetSize();
  // 乐观锁需要预判，如果即将满，则放弃之前的乐观锁过程，悲观走一次（全部获取写锁且不释放）
  int new_size = leaf->Insert(key, value, comparator_);
  bool inserted = (new_size > old_size);
  if (inserted) {
    if (new_size == leaf_max_size_) {
      // std::cout<<transaction->GetThreadId()<<" split the page"<<std::endl;
      LeafPage *new_leaf_ptr = Split(leaf, transaction);
      new_leaf_ptr->SetNextPageId(leaf->GetNextPageId());
      leaf->SetNextPageId(new_leaf_ptr->GetPageId());
      InsertIntoParent(leaf, new_leaf_ptr->KeyAt(0), new_leaf_ptr, transaction);
      return true;
    }
  } else {
    if (transaction == nullptr) {
      buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    }
    return false;
  }
  return inserted;
}

/*
 * 使用后要unpin返回的那一页
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node, Transaction *transaction) {
  page_id_t new_page_id;
  Page *page = buffer_pool_manager_->NewPage(&new_page_id);
  // std::cout<<transaction->GetThreadId()<<" in split for page "<<node->GetPageId()<<std::endl;
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "out of memory when start to split");
  }
  if (transaction != nullptr) {
    // std::cout<<"up split\n";
    page->WLatch();
    // std::cout<<"down split\n";
    transaction->AddIntoPageSet(page);
  }
  if (node->IsLeafPage()) {
    LeafPage *leaf_page = reinterpret_cast<LeafPage *>(node);
    LeafPage *new_leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
    new_leaf_page->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);
    leaf_page->MoveHalfTo(new_leaf_page);
    // std::cout<<transaction->GetThreadId()<<" out leaf split for page "<<node->GetPageId()<<std::endl;
    return reinterpret_cast<N *>(new_leaf_page);
  }
  InternalPage *inter_page = reinterpret_cast<InternalPage *>(node);
  InternalPage *new_inter_page = reinterpret_cast<InternalPage *>(page->GetData());
  new_inter_page->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);
  inter_page->MoveHalfTo(new_inter_page, buffer_pool_manager_);
  // std::cout<<transaction->GetThreadId()<<" out inter split for page "<<node->GetPageId()<<std::endl;
  return reinterpret_cast<N *>(new_inter_page);
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  // std::cout<<transaction->GetThreadId()<<" in insertintoParent for page "<<old_node->GetPageId()<<std::endl;
  root_latch_.lock();
  if (old_node->IsRootPage()) {
    page_id_t new_page_id;  // 这里有必要用一个新变量，不然则先改动了root_page_id，再拿root_page锁，中间可能被先拿去了。
    Page *new_root_page = buffer_pool_manager_->NewPage(&new_page_id);
    if (transaction != nullptr) {
      // std::cout<<"up inserttop\n";
      new_root_page->WLatch();
      // std::cout<<"down inserttop\n";
      transaction->AddIntoPageSet(new_root_page);
    }
    root_page_id_ = new_page_id;
    if (root_page_id_ == -1) {
      assert(false);
    }
    InternalPage *new_root_inter_page = reinterpret_cast<InternalPage *>(new_root_page);
    new_root_inter_page->Init(root_page_id_, root_page_id_, internal_max_size_);
    new_root_inter_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    if (transaction == nullptr) {
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
      buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
    }
    UpdateRootPageId(0);
    root_latch_.unlock();
    // std::cout<<transaction->GetThreadId()<<" out root insertIntoParent for page
    // "<<old_node->GetPageId()<<new_node->GetPageId()<<" to "<<root_page_id_<<std::endl;
    return;
  }
  root_latch_.unlock();
  page_id_t parent_page_id = old_node->GetParentPageId();
  InternalPage *parent_page =
      reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page_id)->GetData());
  int new_size = parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  new_node->SetParentPageId(parent_page_id);
  if (transaction == nullptr) {
    buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
  }
  if (new_size > parent_page->GetMaxSize()) {
    // 处理产生新root节点的问题！
    // std::cout<<"Before Split happened\n";
    // Print();
    InternalPage *new_inter_page = Split(parent_page, transaction);
    // std::cout<<"Split happened\n";
    // Print();
    InsertIntoParent(parent_page, new_inter_page->KeyAt(0), new_inter_page, transaction);
  } else {
    // if (transaction == nullptr) {
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
    // }
  }
  // std::cout<<transaction->GetThreadId()<<" out normal insertIntoParent for page
  // "<<old_node->GetPageId()<<new_node->GetPageId()<<" to "<<parent_page_id<<std::endl;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // if(transaction != nullptr) {std::cout<<transaction->GetThreadId()<<" in Remove "<<std::endl;}
  // root_latch_.lock();
  // if (IsEmpty()) {
  //   root_latch_.unlock();
  //   return;
  // }
  // root_latch_.unlock();
  Page *leaf_page = FindLeafPage(key, false, Operation::DELETE, transaction);
  LeafPage *leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  if (leaf->GetPageId() == -1 || leaf->GetParentPageId() == -1) {
    // std::cout<<"warning\n";
  }
  int old_size = leaf->GetSize();
  int new_size = leaf->RemoveAndDeleteRecord(key, comparator_);
  if (old_size == new_size) {
    // printf("BPLUSTREE_TYPE::Remove: delete key not exist\n");
    if (transaction == nullptr) {
      buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    } else {
      UnpinAndUnLatch(Operation::DELETE, transaction);
    }
    return;
  }
  // bool page_coalesce_deleted = false;
  if (leaf->GetSize() < leaf->GetMinSize()) {
    // 持有root锁，当前页写锁，可能持有parent写锁
    // page_coalesce_deleted = CoalesceOrRedistribute(leaf, transaction);
    CoalesceOrRedistribute(leaf, transaction);
  } else {
    if (transaction == nullptr) {
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    }
  }
  UnpinAndUnLatch(Operation::DELETE, transaction);
  // if(transaction != nullptr) {std::cout<<transaction->GetThreadId()<<" out Remove "<<std::endl;}
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 * 判断左右兄弟是否能合并，能则合并过去，不能则redistribute
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  // root_latch_.lock();
  [[maybe_unused]] page_id_t old_parent_id = node->GetParentPageId();
  if (node->IsRootPage()) {
    bool del = AdjustRoot(node, transaction);
    // root_latch_.unlock();
    return del;
  }
  // root_latch_.unlock();
  page_id_t parent_id = node->GetParentPageId();
  page_id_t pre_id = INVALID_PAGE_ID;
  page_id_t next_id = INVALID_PAGE_ID;
  Page *pre_page;
  Page *next_page;
  N *pre_ptr;
  N *next_ptr;

  Page *parent_page = buffer_pool_manager_->FetchPage(parent_id);
  InternalPage *parent_ptr = reinterpret_cast<InternalPage *>(parent_page);
  int node_index_in_parent = parent_ptr->ValueIndex(node->GetPageId());
  if (parent_id < 0 || node_index_in_parent < 0) {
    // std::cout<<"warning\n";
  }
  // 若node有左兄弟
  if (node_index_in_parent > 0) {
    pre_id = parent_ptr->ValueAt(node_index_in_parent - 1);
    pre_page = buffer_pool_manager_->FetchPage(pre_id);
    pre_ptr = reinterpret_cast<N *>(pre_page);
    pre_page->WLatch();
    transaction->AddIntoPageSet(pre_page);
    if (pre_ptr->GetSize() + node->GetSize() >= node->GetMaxSize()) {
      // pre_page->WLatch();
      // transaction->AddIntoPageSet(pre_page);
      Redistribute(pre_ptr, node, 1);
      return false;
    }
  }
  if (node_index_in_parent != parent_ptr->GetSize() - 1) {
    next_id = parent_ptr->ValueAt(node_index_in_parent + 1);
    next_page = buffer_pool_manager_->FetchPage(next_id);
    next_ptr = reinterpret_cast<N *>(next_page);
    next_page->WLatch();
    transaction->AddIntoPageSet(next_page);
    if (next_ptr->GetSize() + node->GetSize() >= node->GetMaxSize()) {
      // next_page->WLatch();
      // transaction->AddIntoPageSet(next_page);
      Redistribute(next_ptr, node, 0);
      return false;
    }
  }
  if (pre_id != INVALID_PAGE_ID) {
    // pre_page = buffer_pool_manager_->FetchPage(pre_id);
    // pre_page->WLatch();
    // transaction->AddIntoPageSet(pre_page);
    // std::cout<<"Before Coalesce happened\n";
    // Print();
    Coalesce(&pre_ptr, &node, &parent_ptr, node_index_in_parent, transaction);
    // std::cout<<"Coalesce happened\n";
    // Print();
  } else {
    // next_page = buffer_pool_manager_->FetchPage(next_id);
    // next_page->WLatch();
    // transaction->AddIntoPageSet(next_page);
    // std::cout<<"Before Coalesce happened\n";
    // Print();
    Coalesce(&next_ptr, &node, &parent_ptr, node_index_in_parent, transaction, false);
    // std::cout<<"Coalesce happened\n";
    // Print();
  }
  return true;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction, bool ToLeft) {
  // std::cout<<transaction->GetThreadId()<<" in coalesce for page "<<(*node)->GetPageId()<<"
  // "<<(*neighbor_node)->GetPageId()<<" to "<<(*parent)->GetPageId()<<std::endl;
  if (ToLeft) {
    if ((*node)->IsLeafPage()) {
      LeafPage *leaf_page = reinterpret_cast<LeafPage *>(*node);
      LeafPage *neighbor_leaf_page = reinterpret_cast<LeafPage *>(*neighbor_node);
      leaf_page->MoveAllTo(neighbor_leaf_page);
      neighbor_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
    } else {
      KeyType middle_key = (*parent)->KeyAt(index);
      InternalPage *inter_page = reinterpret_cast<InternalPage *>(*node);
      InternalPage *neighbor_inter_page = reinterpret_cast<InternalPage *>(*neighbor_node);
      (inter_page)->MoveAllTo(neighbor_inter_page, middle_key, buffer_pool_manager_);
    }
  } else {
    if ((*node)->IsLeafPage()) {
      LeafPage *leaf_page = reinterpret_cast<LeafPage *>(*node);
      LeafPage *neighbor_leaf_page = reinterpret_cast<LeafPage *>(*neighbor_node);
      leaf_page->MoveAllTo(neighbor_leaf_page, false);
    } else {
      KeyType middle_key =
          (*parent)->KeyAt(index + 1);  // 注意细节，往右合并，拿下来的key是在index+1位置的，是原本对应于兄弟的
      InternalPage *inter_page = reinterpret_cast<InternalPage *>(*node);
      InternalPage *neighbor_inter_page = reinterpret_cast<InternalPage *>(*neighbor_node);
      (inter_page)->MoveAllTo(neighbor_inter_page, middle_key, buffer_pool_manager_, false);
    }
  }
  (*parent)->Remove(index);
  if (transaction == nullptr) {
    buffer_pool_manager_->UnpinPage((*node)->GetPageId(), true);
    buffer_pool_manager_->UnpinPage((*neighbor_node)->GetPageId(), true);
    assert(buffer_pool_manager_->DeletePage((*node)->GetPageId()));
  } else {
    transaction->AddIntoDeletedPageSet((*node)->GetPageId());
  }
  if ((*parent)->GetSize() < (*parent)->GetMinSize()) {
    // std::cout<<transaction->GetThreadId()<<" out coalesce for page
    // "<<(*node)->GetPageId()<<(*neighbor_node)->GetPageId()<<" to "<<(*parent)->GetPageId()<<std::endl;
    return CoalesceOrRedistribute((*parent), transaction);
  }
  // std::cout<<transaction->GetThreadId()<<" out coalesce for page
  // "<<(*node)->GetPageId()<<(*neighbor_node)->GetPageId()<<" to "<<(*parent)->GetPageId()<<std::endl;
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * 即0表示node在左边，否则node在右边。
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  // printf("entering Redistribute\n");
  // 这两个变量用于告诉父节点，应该修改自己的哪个KV对，修改后的key应该来自sibling page的哪儿
  int new_index;
  page_id_t child_page_id;
  if (index == 0) {
    // leaf在左边，parent中变动的是原本存有sibling的第一个Key的KV对，因为这个转到leaf中去了
    new_index = 1;
    child_page_id = reinterpret_cast<LeafPage *>(neighbor_node)->GetPageId();
  } else {
    new_index = reinterpret_cast<LeafPage *>(neighbor_node)->GetSize() - 1;
    child_page_id = reinterpret_cast<LeafPage *>(node)->GetPageId();
  }
  if (node->IsLeafPage()) {
    LeafPage *leaf_page = reinterpret_cast<LeafPage *>(node);
    LeafPage *sibling_page = reinterpret_cast<LeafPage *>(neighbor_node);
    Page *page = buffer_pool_manager_->FetchPage(leaf_page->GetParentPageId());
    InternalPage *parent_page = reinterpret_cast<InternalPage *>(page->GetData());
    // leaf在左边时，parent的middle_key是sibling的KeyAt(0)，middle_value是sibling的page_id
    // redistribute后，middle_key = sibling->KeyAt(1), middle_value不变
    // leaf在右边时，parent的middle_key是leaf的KeyAt(0)，middle_value是leaf的page_id
    // redistribute后，middle_key = sibling->KeyAt(size_ - 1), middle_value不变
    int middle_idx = parent_page->ValueIndex(child_page_id);
    KeyType to_parent_key = sibling_page->KeyAt(new_index);
    parent_page->SetKeyAt(middle_idx, to_parent_key);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    if (index == 0) {
      sibling_page->MoveFirstToEndOf(leaf_page);
    } else {
      sibling_page->MoveLastToFrontOf(leaf_page);
    }
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
    return;
  }
  InternalPage *inter_page = reinterpret_cast<InternalPage *>(node);
  InternalPage *sibling_page = reinterpret_cast<InternalPage *>(neighbor_node);
  Page *page = buffer_pool_manager_->FetchPage(inter_page->GetParentPageId());
  InternalPage *parent_page = reinterpret_cast<InternalPage *>(page->GetData());
  int middle_idx = parent_page->ValueIndex(child_page_id);
  KeyType middle_key = parent_page->KeyAt(middle_idx);
  KeyType to_parent_key = sibling_page->KeyAt(new_index);
  parent_page->SetKeyAt(middle_idx, to_parent_key);
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  if (index == 0) {
    sibling_page->MoveFirstToEndOf(inter_page, middle_key, buffer_pool_manager_);
  } else {
    sibling_page->MoveLastToFrontOf(inter_page, middle_key, buffer_pool_manager_);
  }
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 * 我对这个函数的使用假设是，对根节点进行delete前若发现size为1，则要调用，来更换/删除根节点
 * 在CoalesceOrRedistribute中调用，此时可能不需要操作，因为root没有value数量限制。
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *node, Transaction *transaction) {
  if (node->IsLeafPage()) {
    if (node->GetSize() == 0) {
      if (transaction == nullptr) {
        buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
        assert(buffer_pool_manager_->DeletePage(node->GetPageId()));
      } else {
        transaction->AddIntoDeletedPageSet(node->GetPageId());
      }
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(false);
      return true;
    }
    return false;
  }
  if (node->GetSize() == 1) {
    // InternalPage* node = reinterpret_cast<InternalPage*>(node);
    BPlusTreePage *new_root_node = reinterpret_cast<BPlusTreePage *>(
        buffer_pool_manager_->FetchPage(reinterpret_cast<InternalPage *>(node)->ValueAt(0))->GetData());
    if (new_root_node == nullptr) {
      throw std::runtime_error("fetch failed");
    }
    new_root_node->SetParentPageId(new_root_node->GetPageId());
    root_page_id_ = new_root_node->GetPageId();
    UpdateRootPageId();
    buffer_pool_manager_->UnpinPage(new_root_node->GetPageId(), true);
    if (transaction == nullptr) {
      buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
      assert(buffer_pool_manager_->DeletePage(node->GetPageId()));
    } else {
      transaction->AddIntoDeletedPageSet(node->GetPageId());
    }
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  if (IsEmpty()) {
    return end();
  }
  KeyType key{};
  Page *page = FindLeafPage(key, true);
  return INDEXITERATOR_TYPE(0, page, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  if (IsEmpty()) {
    return end();
  }
  Page *page = FindLeafPage(key, false);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  int index;
  index = leaf_page->KeyIndex(key, comparator_);
  // 如果key超过了所有叶节点的所有key，则返回end()
  if (index != -1) {
    return INDEXITERATOR_TYPE(index, page, buffer_pool_manager_);
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return end();
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { return INDEXITERATOR_TYPE(-1, nullptr, buffer_pool_manager_); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * 返回的Page，是没有unpin的，使用后可能需要unpin
 */

INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost, Operation op, Transaction *transaction) {
  if (transaction != nullptr) {
    // std::cout<<transaction->GetThreadId()<<" in FindleafPage "<<std::endl;
    root_latch_.lock();
  }
  if (IsEmpty()) {
    if (transaction != nullptr) {
      root_latch_.unlock();
    }
    return nullptr;
  }
  if (transaction != nullptr) {
    root_latch_.unlock();
  }
  Page *cur_page = buffer_pool_manager_->FetchPage(root_page_id_);
  if (cur_page == nullptr) {
    assert(false);
  }
  if (transaction != nullptr) {
    if (op == Operation::READ) {
      cur_page->RLatch();
    } else {
      // std::cout<<transaction->GetThreadId()<<" Get latch on page "<<cur_page->GetPageId()<<"\n";
      cur_page->WLatch();
    }
    transaction->AddIntoPageSet(cur_page);
  }

  // 这里是为了避免根节点即将出现变化时，多线程同时来到，于是让其余线程重来。
  if (cur_page->GetPageId() != root_page_id_) {
    if (transaction != nullptr) {
      UnpinAndUnLatch(op, transaction);
    }
    return FindLeafPage(key, leftMost, op, transaction);
  }

  BPlusTreePage *cur_node = reinterpret_cast<InternalPage *>(cur_page);
  while (!cur_node->IsLeafPage()) {
    InternalPage *inter_ = reinterpret_cast<InternalPage *>(cur_page);
    page_id_t child_page_id;
    if (leftMost) {
      child_page_id = inter_->ValueAt(0);
    } else {
      child_page_id = inter_->Lookup(key, comparator_);
    }
    // 更新cur_page, cur_node, page_id, 将原page_id对应页unpin, fetch新页。
    Page *child_page = buffer_pool_manager_->FetchPage(child_page_id);
    if (child_page == nullptr) {
      assert(false);
    }
    if (transaction != nullptr) {
      if (op == Operation::READ) {
        child_page->RLatch();
        UnpinAndUnLatch(op, transaction);
      } else {
        // Print();
        // std::cout<<transaction->GetThreadId()<<" Get latch page "<<child_page_id<<std::endl;
        child_page->WLatch();
        // std::cout<<"down while\n";
      }
    }
    BPlusTreePage *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    if (op != Operation::READ && transaction != nullptr && IsSafe(child_node, op)) {
      UnpinAndUnLatch(op, transaction);
    }
    if (transaction == nullptr) {
      buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);
    } else {
      transaction->AddIntoPageSet(child_page);
    }
    // old_page_id = page_id;
    cur_page = child_page;
    cur_node = child_node;
  }
  if (transaction != nullptr) {
    // std::cout<<transaction->GetThreadId()<<" in FindleafPage get page "<<cur_page->GetPageId()<<std::endl;
  }
  return cur_page;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnpinAndUnLatch(Operation op, Transaction *transaction) {
  if (transaction == nullptr) {
    return;
  }
  for (Page *page : *transaction->GetPageSet()) {
    if (op == Operation::READ) {
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    } else {
      page->WUnlatch();
      // std::cout<<transaction->GetThreadId()<<" release wlatch on page "<<page->GetPageId()<<std::endl;
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    }
    // if(page->GetPageId() == root_page_id_) {
    //   root_latch_.unlock();
    // }
  }
  transaction->GetPageSet()->clear();
  for (page_id_t page_id : *transaction->GetDeletedPageSet()) {
    buffer_pool_manager_->DeletePage(page_id);
  }
  transaction->GetDeletedPageSet()->clear();
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsSafe(BPlusTreePage *node, Operation op) {
  if (op == Operation::INSERT) {
    if (node->IsLeafPage()) {
      LeafPage *leaf = reinterpret_cast<LeafPage *>(node);
      return (leaf->GetSize() < leaf->GetMaxSize() - 1);
    }
    InternalPage *inter = reinterpret_cast<InternalPage *>(node);
    return (inter->GetSize() < inter->GetMaxSize());
  }
  if (node->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(node);
    return (leaf->GetSize() > leaf->GetMaxSize() / 2);
  }
  InternalPage *inter = reinterpret_cast<InternalPage *>(node);
  return (inter->GetSize() > (inter->GetMaxSize() + 1) / 2);
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
