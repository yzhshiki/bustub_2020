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

#include <string>

#include "storage/index/b_plus_tree.h"
#include "common/exception.h"
#include "common/rid.h"
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
  root_latch_.lock();
  // LOG_DEBUG("Getting value of key: %ld\n", key.ToString());
  if (IsEmpty()) {
    root_latch_.unlock();
    return false;
  }
  root_latch_.unlock();
  Page *page = FindLeafPage(key, false);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page);
  ValueType value{};
  bool exist;
  exist = leaf_page->Lookup(key, &value, comparator_);
  if (exist) {
    result->push_back(value);
  }
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
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
  // std::cout<<transaction->GetThreadId()<<" Insert getting root latch\n";
  root_latch_.lock();
  // std::cout<<transaction->GetThreadId()<< "Insert got root latch\n";
  if (IsEmpty()) {
    // std::cout<<transaction->GetThreadId()<< "Start new tree\n";
    StartNewTree(key, value);
    // std::cout<<transaction->GetThreadId()<<" Insert release root latch and leave insert\n";
    root_latch_.unlock();
    return true;
  }
  // root_latch_.unlock();
  bool ans = InsertIntoLeaf(key, value, transaction, false);
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
  page->WLatch();
  if (page == nullptr) {
    page->WUnlatch();
    throw Exception(ExceptionType::OUT_OF_MEMORY, "out of memory when start new tree");
  }
  root_page_id_ = page->GetPageId();
  LeafPage *root_page = reinterpret_cast<LeafPage *>(page);
  root_page->Init(root_page_id_, root_page_id_, leaf_max_size_);
  root_page->Insert(key, value, comparator_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
  // assert(page->GetPinCount() == 0);
  UpdateRootPageId(1);
  // LOG_DEBUG("StartNewTree id: %d, with key: %ld, value: %ld\n", root_page_id_, key.ToString(), value.Get());
  // Print(buffer_pool_manager_);
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
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction, bool notfull) {
  // Page *page = FindLeafPage(key, false);
  // std::cout<<"enter InsertIntoLeaf"<<std::endl;
  FindLeafPageInTran(key, false, transaction, notfull);
  std::shared_ptr<std::deque<Page *>> Ancestors = transaction->GetPageSet();
  Page *page = Ancestors->back();
  // if(page->GetPageId() > 500) {
  //   std::cout<<"before insert\n";
  //   std::cout<<"page id "<<page->GetPageId()<<std::endl;
  //   Print();
  // }
  // std::cout<<"before insert\n";
  // std::cout<<"page id "<<page->GetPageId()<<std::endl;
  Ancestors->pop_back();
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page);
  if(leaf_page->GetPageId() == 0) {
    Print(buffer_pool_manager_);
    assert(false);
  }
  if (leaf_page->checkDupl(key, comparator_)) {
    releaseAncestorLocks(transaction);
    // std::cout<<transaction->GetThreadId()<<" unlatch page "<<page->GetPageId()<<" in checkDupl"<<std::endl;
    // std::cout<<transaction->GetThreadId()<<" Insert release root latch and leave insert\n";
    page->WUnlatch();
    root_latch_.unlock();
    return false;
  }
  int old_size = leaf_page->GetSize();
  // 乐观锁需要预判，如果即将满，则放弃之前的乐观锁过程，悲观走一次（全部获取写锁且不释放）
  // if(old_size == leaf_max_size_ - 1 && notfull) {
  //   std::cout<<"start to pessmistic\n";
  //   std::cout<<transaction->GetThreadId()<<std::endl;
  //   page->WUnlatch();
  //   return InsertIntoLeaf(key, value, transaction, false);
  // }
  leaf_page->Insert(key, value, comparator_);
  int new_size = leaf_page->GetSize();
  bool inserted = (new_size > old_size);
  if (inserted) {
    // std::cout<<transaction->GetThreadId()<<std::endl;
    // std::cout<<"Insert key "<<key.ToString()<<std::endl;
    if (new_size == leaf_max_size_) {
      // std::cout<<transaction->GetThreadId()<<" start to split\n";
      // root_latch_.lock();
      LeafPage *new_leaf_ptr = Split(leaf_page);
      InsertIntoParent(leaf_page, new_leaf_ptr->KeyAt(0), new_leaf_ptr, transaction);
      // std::cout<<transaction->GetThreadId()<<" ended split\n";
    }
  }
  // assert(page->GetPinCount() == 1);
  
  releaseAncestorLocks(transaction);
  // if(page->GetPageId() > 500) {
  //   std::cout<<"after insert\n";
  //   std::cout<<"page id "<<page->GetPageId()<<std::endl;
  //   Print();
  //   assert(false);  
  // }
  // std::cout<<transaction->GetThreadId()<<" unlatch page "<<page->GetPageId()<<" after insertIntoLeaf"<<std::endl;
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), inserted);
  // LOG_DEBUG("InsertIntoLeaf id: %d with key: %ld, value: %ld\n",leaf_page->GetPageId(), key.ToString(),
  // value.Get());
  // std::cout<<transaction->GetThreadId()<<" Insert release root latch and leave insert\n";
  root_latch_.unlock();
  // std::cout<<"Insert released root latch\n";
  // std::cout<<"leave InsertIntoLeaf"<<std::endl;
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
N *BPLUSTREE_TYPE::Split(N *node) {
  // LOG_DEBUG("start to split id: %d\n",node->GetPageId());
  // std::cout<<"start to split new nod/e\n";
  // std::cout<<"*******before split*******\n";
  // Print();
  page_id_t new_page_id;
  Page *page = buffer_pool_manager_->NewPage(&new_page_id);
  // std::cout<<"Split getting page "<<page->GetPageId()<<" latch\n";
  page->WLatch();
  // std::cout<<"Split got page "<<page->GetPageId()<<" latch\n";
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "out of memory when start to split");
  }
  if (node->IsLeafPage()) {
    LeafPage *leaf_page = reinterpret_cast<LeafPage *>(node);
    LeafPage *new_leaf_page = reinterpret_cast<LeafPage *>(page);
    new_leaf_page->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);
    // new_leaf_page->SetParentPageId(leaf_page->GetParentPageId());
    new_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
    leaf_page->SetNextPageId(new_page_id);
    leaf_page->MoveHalfTo(new_leaf_page);
    // assert(page->GetPinCount() == 1);
    // std::cout<<"end split new node\n";
    // std::cout<<"*******After split*******\n";
    // Print();
    // std::cout<<"*******End split*******\n";
    return reinterpret_cast<N *>(new_leaf_page);
  }
  InternalPage *inter_page = reinterpret_cast<InternalPage *>(node);
  InternalPage *new_inter_page = reinterpret_cast<InternalPage *>(page);
  new_inter_page->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);
  // new_inter_page->SetParentPageId(inter_page->GetParentPageId());
  inter_page->MoveHalfTo(new_inter_page, buffer_pool_manager_);
  // assert(page->GetPinCount() == 1);
  // std::cout<<"end split new node\n";
  // std::cout<<"*******After split*******\n"; 
  // Print();
  // std::cout<<"*******End split*******\n";
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
                                        // std::cout<<"start to insertIntoParent\n";
  // std::cout<<"*******before InsertIntoParent*******\n"; 
  // Print();
  if (old_node->IsRootPage()) {
    // std::cout<<"Insert Into Parent and create new root page\n";
    // std::cout<<"*******start up root page*******\n";
    // Print();
    Page *new_root_page = buffer_pool_manager_->NewPage(&root_page_id_);
    // std::cout<<transaction->GetThreadId()<<" getting page "<<new_root_page->GetPageId()<<" latch in InsertInroParent\n";
    new_root_page->WLatch();
    // std::cout<<transaction->GetThreadId()<<" got page "<<new_root_page->GetPageId()<<" latch in InsertInroParent\n";
    InternalPage *new_root_inter_page = reinterpret_cast<InternalPage *>(new_root_page);
    new_root_inter_page->Init(root_page_id_, root_page_id_, internal_max_size_);
    new_root_inter_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    // assert(new_root_page->GetPinCount() == 1);
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    // std::cout<<"*******after up root page*******\n";
    // Print();
    // std::cout<<"*******End up root page*******\n";
    // std::cout<<transaction->GetThreadId()<<"release page "<<new_root_page->GetPageId()<<" latch after InsertInroParent\n";
    new_root_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    reinterpret_cast<Page *>(new_node)->WUnlatch();
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    UpdateRootPageId(0);
    // root_latch_.unlock();
    // std::cout<<"end insertIntoParent\n";
    return;
  }
  // root_latch_.unlock();
  page_id_t parent_id = old_node->GetParentPageId();
  std::shared_ptr<std::deque<Page *>> Ancestors = transaction->GetPageSet();
  Page *page = Ancestors->back();
  Ancestors->pop_back();
  InternalPage *parent_page = reinterpret_cast<InternalPage *>(page);
  // if(parent_page->IsRootPage()) {
  //   std::cout<<"insert "<<key.ToString()<<" : "<<new_node->GetPageId()<<" to parent, root size: "<<parent_page->GetSize()<<std::endl;
  // }
  int new_size = parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  if(new_node->GetPageId() == 0) {
    std::cout<<"insert 0 to parent\n";
    assert(false);
  }
  new_node->SetParentPageId(parent_id);
  // assert(page->GetPinCount() == 1);
  if (new_size > parent_page->GetMaxSize()) {
    // 处理产生新root节点的问题！
    // root_latch_.lock();
    InternalPage *new_inter_page = Split(parent_page);
    InsertIntoParent(parent_page, new_inter_page->KeyAt(0), new_inter_page, transaction);
    // buffer_pool_manager_->UnpinPage(new_inter_page->GetPageId(), true);
  }
  // std::cout<<"*******After InsertIntoParent*******\n"; 
  // Print();
  // std::cout<<"*******End InsertIntoParent*******\n";
  releaseAncestorLocks(transaction);
  // std::cout<<transaction->GetThreadId()<<"release page "<<page->GetPageId()<<" latch after InsertInroParent\n";
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(parent_id, true);
  // std::cout<<transaction->GetThreadId()<<"release page "<<new_node->GetPageId()<<" latch after InsertInroParent\n";
  reinterpret_cast<Page *>(new_node)->WUnlatch();
  buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
  // std::cout<<"end insertIntoParent\n";
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
  // std::cout<<transaction->GetThreadId()<<" remove getting root latch\n";
  root_latch_.lock();
  // std::cout<<transaction->GetThreadId()<<" remove got root latch\n";
  if (IsEmpty()) {
    // std::cout<<transaction->GetThreadId()<<" remove release root latch and leave remove\n";
    root_latch_.unlock();
    return;
  }
  // root_latch_.unlock();
  FindLeafPageInTran(key, false, transaction, false);
  std::shared_ptr<std::deque<Page *>> Ancestors = transaction->GetPageSet();
  Page *page = Ancestors->back();
  Ancestors->pop_back();
  // page->RUnlatch();
  // page->WLatch();
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page);
  // printf("entering remove ans delete record\n");
  // std::cout<<transaction->GetThreadId()<<std::endl;
  // std::cout<<"root page id "<<root_page_id_<<std::endl;
  // std::cout<<"remove "<<key.ToString()<<" in page "<<leaf_page->GetPageId()<<std::endl;
  leaf_page->RemoveAndDeleteRecord(key, comparator_);
  // Print(buffer_pool_manager_);
  if (leaf_page->GetSize() < leaf_page->GetMinSize()) {
    // root_latch_.lock();
    // if(!leaf_page->IsRootPage()) {
    //   Page *parent_page = Ancestors->back();
    //   parent_page->WLatch();
    // }
    // 持有root锁，当前页写锁，可能持有parent写锁
    // LOG_DEBUG("entering cor\n");
    // std::cout<<transaction->GetThreadId()<<" enter CoalesceOrRedistribute\n";
    CoalesceOrRedistribute(leaf_page, transaction);
    // std::cout<<transaction->GetThreadId()<<" leave CoalesceOrRedistribute\n";
  }
  releaseAncestorLocks(transaction);
  // std::cout<<transaction->GetThreadId()<<" unlatch page "<<page->GetPageId()<<std::endl;
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  // std::cout<<"remove release root latch\n";
  // std::cout<<transaction->GetThreadId()<<" remove release root latch and leave remove\n";
  root_latch_.unlock();
  // std::cout<<"remove released root latch\n";
  // printf("leaving remove\n");
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
  // printf("entering CoalesceOrRedistribute\n");
  if (node->IsRootPage()) {
    bool del = AdjustRoot(node);
    // root_latch_.unlock();
    // printf("leaving CoalesceOrRedistribute after adjustroot\n");
    return del;
  } 
  page_id_t parent_id = node->GetParentPageId();
  page_id_t pre_id = INVALID_PAGE_ID;
  page_id_t next_id = INVALID_PAGE_ID;
  Page *pre_page;
  Page *next_page;
  N *pre_ptr;
  N *next_ptr;

  // std::shared_ptr<std::deque<Page *>> Ancestors = transaction->GetPageSet();
  // Page *parent_page = Ancestors->back();
  // Ancestors->pop_back();
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_id);
  InternalPage *parent_ptr = reinterpret_cast<InternalPage *>(parent_page);
  int node_index_in_parent = parent_ptr->ValueIndex(node->GetPageId());
  assert(node_index_in_parent >= 0);
  // 若node有左兄弟
  if (node_index_in_parent > 0) {
    pre_id = parent_ptr->ValueAt(node_index_in_parent - 1);
    pre_page = buffer_pool_manager_->FetchPage(pre_id);
    // std::cout<<"before ask pre latch\n";
    // pre_page->WLatch();
    // std::cout<<"after ask pre latch\n";
    pre_ptr = reinterpret_cast<N *>(pre_page);
    if (pre_ptr->GetSize() + node->GetSize() >= node->GetMaxSize()) {
      Redistribute(pre_ptr, node, 1);
      // parent_page->WUnlatch();
      // std::cout<<"release pre latch\n";
      // pre_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(pre_id, true);
      buffer_pool_manager_->UnpinPage(parent_id, true);
      // printf("leaving CoalesceOrRedistribute\n");
      return false;
    }
    // std::cout<<"release pre latch\n";
    // pre_page->WUnlatch();
  }
  if (node_index_in_parent != parent_ptr->GetSize() - 1) {
    next_id = parent_ptr->ValueAt(node_index_in_parent + 1);
    // if(next_id == root_page_id_) {
    //   std::cout<<"wow\n";
    // }
    next_page = buffer_pool_manager_->FetchPage(next_id);
    // std::cout<<"before ask next latch\n";
    // next_page->WLatch();
    // std::cout<<"after ask next latch\n";
    next_ptr = reinterpret_cast<N *>(next_page);
    if (next_ptr->GetSize() + node->GetSize() >= node->GetMaxSize()) {
      Redistribute(next_ptr, node, 0);
      // parent_page->WUnlatch();
      // std::cout<<"release next latch\n";
      // next_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(next_id, true);
      buffer_pool_manager_->UnpinPage(parent_id, true);
      // printf("leaving CoalesceOrRedistribute\n");
      return false;
    }
    // std::cout<<"release next latch\n";
    // next_page->WUnlatch();
  }
  // std::cout<<"*******before merge*******\n";
  // Print();
  // std::cout<<"after try redistribute\n";
  if (pre_id != INVALID_PAGE_ID) {
    // pre_page->WLatch();
    Coalesce(&pre_ptr, &node, &parent_ptr, node_index_in_parent, transaction);
  } else {
    // assert(false);
    // next_page->WLatch();
    Coalesce(&next_ptr, &node, &parent_ptr, node_index_in_parent, transaction, false);
  }
  // std::cout<<"*******After merge*******\n";
  // Print();
  // std::cout<<"*******End merge*******\n";
  // std::cout<<"after try coalesce\n";
  releaseAncestorLocks(transaction);
  if (pre_id != INVALID_PAGE_ID) {
    // parent_page->WUnlatch();
    // pre_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(pre_id, true);
  }
  if (next_id != INVALID_PAGE_ID) {
    // parent_page->WUnlatch();
    // next_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(next_id, true);
  }
  // buffer_pool_manager_->UnpinPage(parent_id, true);
  // printf("leaving CoalesceOrRedistribute\n");
  return false;
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
  // printf("enter Coalesce\n");
  // std::cout<<"merge page "<<reinterpret_cast<BPlusTreePage*>(*neighbor_node)->GetPageId()<<" and page "<<reinterpret_cast<BPlusTreePage*>(*node)->GetPageId()<<std::endl;                              
  // if(reinterpret_cast<BPlusTreePage*>(*neighbor_node)->GetPageId() == 3 && reinterpret_cast<BPlusTreePage*>(*node)->GetPageId() == 1) {
  //   std::cout<<"got it\n";
  // }
  // reinterpret_cast<Page *>(*neighbor_node)->WLatch();
  if(ToLeft) {
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
      KeyType middle_key = (*parent)->KeyAt(index + 1); //注意细节，往右合并，拿下来的key是在index+1位置的，是原本对应于兄弟的
      InternalPage *inter_page = reinterpret_cast<InternalPage *>(*node);
      InternalPage *neighbor_inter_page = reinterpret_cast<InternalPage *>(*neighbor_node);
      (inter_page)->MoveAllTo(neighbor_inter_page, middle_key, buffer_pool_manager_, false);
    }
  }
  (*parent)->Remove(index);
  if((*parent)->GetSize() == 0) {
    Print(buffer_pool_manager_);
    assert(false);
  }
  if ((*parent)->GetSize() < (*parent)->GetMinSize()) {
    // printf("leaving Coalesce and goto cor parent\n");  
    return CoalesceOrRedistribute((*parent), transaction);
  }
  // buffer_pool_manager_->UnpinPage((*parent)->GetPageId(), true);
  // printf("leaving Coalesce\n");  
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
    InternalPage *parent_page = reinterpret_cast<InternalPage *>(page);
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
    // printf("leaving Redistribute\n"); 
    return;
  }
  InternalPage *inter_page = reinterpret_cast<InternalPage *>(node);
  InternalPage *sibling_page = reinterpret_cast<InternalPage *>(neighbor_node);
  Page *page = buffer_pool_manager_->FetchPage(inter_page->GetParentPageId());
  InternalPage *parent_page = reinterpret_cast<InternalPage *>(page);
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
  // printf("leaving Redistribute\n"); 
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
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *node) {
  if (IsEmpty()) {
    return false;
  }
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  if (node->IsLeafPage() && node->GetSize() == 0) {
    LeafPage *root_as_leaf = reinterpret_cast<LeafPage *>(page);
    root_as_leaf->Remove(0);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    buffer_pool_manager_->DeletePage(root_page_id_);
    root_page_id_ = INVALID_PAGE_ID;
    return true;
  }
  if (!node->IsLeafPage() && node->GetSize() == 1) {
    InternalPage *root_as_inter = reinterpret_cast<InternalPage *>(page);
    page_id_t new_root_id = root_as_inter->RemoveAndReturnOnlyChild();
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    buffer_pool_manager_->DeletePage(root_page_id_);
    // std::cout<<"*******start down root page*******\n";
    // Print();
    root_page_id_ = new_root_id;
    Page *npage = buffer_pool_manager_->FetchPage(new_root_id);
    InternalPage *new_root_page = reinterpret_cast<InternalPage *>(npage);
    new_root_page->SetParentPageId(new_root_id);
    buffer_pool_manager_->UnpinPage(new_root_id, true);
    UpdateRootPageId(0);
    // std::cout<<"*******after down root page*******\n";
    // Print();
    // std::cout<<"*******End down root page*******\n";
    return true;
  }
  // std::cout<<"root size: "<<node->GetSize()<<std::endl;
  // Print(buffer_pool_manager_);
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
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page);
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
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  // std::cout<<"FindLeafPage getting root latch\n";
  root_latch_.lock();
  // std::cout<<"FindLeafPage got root latch\n";
  Page *parent_page = buffer_pool_manager_->FetchPage(root_page_id_);
  Page *old_parent_page = parent_page;
  // std::cout<<"FindLeafPage getting page "<<old_parent_page->GetPageId()<<" read latch"<<std::endl;
  old_parent_page->RLatch();
  // std::cout<<"FindLeafPage got page "<<old_parent_page->GetPageId()<<" read latch"<<std::endl;
  BPlusTreePage *bpt_page = reinterpret_cast<InternalPage *>(parent_page);
  page_id_t page_id = parent_page->GetPageId();
  while (!bpt_page->IsLeafPage()) {
    InternalPage *bpti_page = reinterpret_cast<InternalPage *>(bpt_page);
    page_id_t child_page_id;
    if (leftMost) {
      child_page_id = bpti_page->ValueAt(0);
    } else {
      child_page_id = bpti_page->Lookup(key, comparator_);
    }
    // 更新parent_page, bpt_page, page_id, 将原page_id对应页unpin, fetch新页。
    buffer_pool_manager_->UnpinPage(page_id, false);
    page_id = child_page_id;
    old_parent_page = parent_page;
    parent_page = buffer_pool_manager_->FetchPage(page_id);
    // std::cout<<"FindLeafPage getting page "<<parent_page->GetPageId()<<" read latch"<<std::endl;
    parent_page->RLatch();
    // std::cout<<"FindLeafPage got page "<<parent_page->GetPageId()<<" read latch"<<std::endl;
    // std::cout<<"FindLeafPage release page "<<old_parent_page->GetPageId()<<" read latch"<<std::endl;
    old_parent_page->RUnlatch();
    bpt_page = reinterpret_cast<InternalPage *>(parent_page);
  }
  // LOG_DEBUG("FindLeafPage id: %d\nLeafPage size is : %d\n", parent_page->GetPageId(), bpt_page->GetSize());
  root_latch_.unlock();
  return parent_page;
  // throw Exception(ExceptionType::NOT_IMPLEMENTED, "Implement this for test");
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::FindLeafPageInTran(const KeyType &key, bool leftMost, Transaction *transaction, bool isSearch) {
  // root_latch_.lock();
  // std::cout<<"enter FindLeafPageInTran "<<transaction->GetThreadId()<<std::endl;
  Page *parent_page = buffer_pool_manager_->FetchPage(root_page_id_);
  // root_latch_.unlock();
  Page *old_parent_page = parent_page;
  page_id_t old_page_id = old_parent_page->GetPageId();
  if(isSearch) {
    old_parent_page->RLatch();
    // root_latch_.unlock();
  }
  else {
    // std::cout<<transaction->GetThreadId()<<" latch page in FindLeafpageInTran "<<old_parent_page->GetPageId()<<std::endl;
    old_parent_page->WLatch();
    // std::cout<<"now root size: "<<reinterpret_cast<BPlusTreePage *>(old_parent_page)->GetSize()<<std::endl;
    // if(old_page_id == 0) {
    //   std::cout<<"add page 0 to pageset at root\n";
    // }
  }
  transaction->AddIntoPageSet(old_parent_page);
  BPlusTreePage *bpt_page = reinterpret_cast<InternalPage *>(parent_page);
  page_id_t page_id = parent_page->GetPageId();
  while (!bpt_page->IsLeafPage()) {
    InternalPage *bpti_page = reinterpret_cast<InternalPage *>(bpt_page);
    page_id_t child_page_id;
    if (leftMost) {
      child_page_id = bpti_page->ValueAt(0);
    } else {
      child_page_id = bpti_page->Lookup(key, comparator_);
      // if(child_page_id == 0 || child_page_id > 500) {
      //   std::cout<<"lookup key "<<key.ToString()<<std::endl;
      //   std::cout<<"root page size: "<<bpti_page->GetSize()<<std::endl;
      //   Print(buffer_pool_manager_);
      //   assert(false);  
      // }
    }
    // 更新parent_page, bpt_page, page_id, 将原page_id对应页unpin, fetch新页。
    page_id = child_page_id;
    old_parent_page = parent_page;
    parent_page = buffer_pool_manager_->FetchPage(page_id);
    if(isSearch) {
      parent_page->RLatch();
      transaction->AddIntoPageSet(parent_page);
      old_parent_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(old_page_id, false);
    }
    else {
      // if(parent_page->GetPageId() > 500) {
      //   std::cout<<"lookup key "<<key.ToString()<<std::endl;
      //   Print();
      //   assert(false);  
      // }
      // std::cout<<transaction->GetThreadId()<<" latch page in FindLeafpageInTran "<<parent_page->GetPageId()<<std::endl;
      parent_page->WLatch();
      // if(page_id == 0) {
      //   std::cout<<"page nums: "<<transaction->GetPageSet()->size()<<std::endl;
      //   std::cout<<"add page 0 to pageset in rout\n";
      // }
      transaction->AddIntoPageSet(parent_page);
    }
    old_page_id = page_id;
    bpt_page = reinterpret_cast<InternalPage *>(parent_page);
  }
  // std::cout<<"leave FindLeafPageInTran "<<transaction->GetThreadId()<<std::endl;
  // LOG_DEBUG("FindLeafPage id: %d\nLeafPage size is : %d\n", parent_page->GetPageId(), bpt_page->GetSize());
  // return parent_page;
  // throw Exception(ExceptionType::NOT_IMPLEMENTED, "Implement this for test");
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::releaseAncestorLocks(Transaction *transaction) {
  std::shared_ptr<std::deque<Page *>> Ancestors = transaction->GetPageSet();
  if(Ancestors->size() == 0) {
    return;
  }
  while(Ancestors->size() != 0) {
    Page *page = Ancestors->front();
    // std::cout<<transaction->GetThreadId()<<" unlatch page in releaseAncestorLocks "<<page->GetPageId()<<std::endl;
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    Ancestors->pop_front();
  }
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
    // create a new record<index_name + root_page_id> in header_page
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
