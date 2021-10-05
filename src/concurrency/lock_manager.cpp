//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"

#include <utility>
#include <vector>

namespace bustub {
// 这些函数的实现时要依据情况抛出异常。
bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  if(txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  std::unique_lock<std::mutex> waitlock(latch_);
  LockRequestQueue &lock_queue = lock_table_[rid];
  auto ToBeBlocked = [&]() {
    return (lock_queue.upgrading_ || rid_exclusive_[rid]) && txn->GetState() != TransactionState::ABORTED;
  };
  while(ToBeBlocked()) {
    for(const auto &lock_request : lock_queue.request_queue_) {
      if(lock_request.lock_mode_ == LockMode::EXCLUSIVE) {
        AddEdge(txn->GetTransactionId(), lock_request.txn_id_);
      }
    }
    tid_to_rid_[txn->GetTransactionId()] = rid;
    lock_queue.cv_.wait(waitlock);
  }
  // 解除当前事务对其他事务的依赖状态
  for(const auto &lock_request : lock_queue.request_queue_) {
    if(lock_request.lock_mode_ == LockMode::EXCLUSIVE) {
      RemoveEdge(txn->GetTransactionId(), lock_request.txn_id_);
    }
  }
  tid_to_rid_.erase(txn->GetTransactionId());
  // 死锁中牺牲
  if(txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
  }
  // 把锁给当前事务
  lock_queue.request_queue_.push_back(LockRequest(txn->GetTransactionId(), LockMode::SHARED));
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  if(txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  std::unique_lock<std::mutex> waitlock(latch_);
  LockRequestQueue &lock_queue = lock_table_[rid];
  auto ToBeBlocked = [&]() {
    return (lock_queue.upgrading_ || !lock_queue.request_queue_.empty() ) && txn->GetState() != TransactionState::ABORTED;
  };
  while(ToBeBlocked()) {
    for(const auto &lock_request : lock_queue.request_queue_) {
      AddEdge(txn->GetTransactionId(), lock_request.txn_id_);
    }
    tid_to_rid_[txn->GetTransactionId()] = rid;
    lock_queue.cv_.wait(waitlock);
  }
  // 解除当前事务对其他事务的依赖状态
  for(const auto &lock_request : lock_queue.request_queue_) {
    RemoveEdge(txn->GetTransactionId(), lock_request.txn_id_);
  }
  tid_to_rid_.erase(txn->GetTransactionId());
  // 可能是死锁中被牺牲了
  if(txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
  }
  // 把锁给当前事务
  lock_queue.request_queue_.push_back(LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE));
  txn->GetExclusiveLockSet()->emplace(rid);
  rid_exclusive_[rid] = true;
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  if(txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  std::unique_lock<std::mutex> waitlock(latch_);
  LockRequestQueue &lock_queue = lock_table_[rid];
  lock_queue.upgrading_ = true;
  auto ToBeBlocked = [&]() {
    return lock_queue.request_queue_.size() != 1 && txn->GetState() != TransactionState::ABORTED;
  };
  while(ToBeBlocked()) {
    for(const auto &lock_request : lock_queue.request_queue_) {
        if(lock_request.txn_id_ != txn->GetTransactionId()) {
            AddEdge(txn->GetTransactionId(), lock_request.txn_id_);
        }
    }
    tid_to_rid_[txn->GetTransactionId()] = rid;
    lock_queue.cv_.wait(waitlock);
  }
//  if(!(lock_queue.request_queue_.size() == 1 && lock_queue.request_queue_.back().lock_mode_ == LockMode::SHARED
//     && lock_queue.request_queue_.back().txn_id_ == txn->GetTransactionId() && rid_exclusive_[rid] == false)) {
//      std::cout<<lock_queue.request_queue_.size()<<std::endl;
//      std::cout<<rid_exclusive_[rid]<<std::endl;
//      assert(lock_queue.request_queue_.size() == 1 && lock_queue.request_queue_.back().lock_mode_ == LockMode::SHARED
//             && lock_queue.request_queue_.back().txn_id_ == txn->GetTransactionId() && rid_exclusive_[rid] == false);
//  }
  // 解除当前事务对其他事务的依赖状态
  for(const auto &lock_request : lock_queue.request_queue_) {
    RemoveEdge(txn->GetTransactionId(), lock_request.txn_id_);
  }
  tid_to_rid_.erase(txn->GetTransactionId());
  // 死锁中牺牲
  if(txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
  }
  // 把锁给当前事务
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  rid_exclusive_[rid] = true;
  lock_queue.upgrading_ = false;
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  if(lock_table_.find(rid) == lock_table_.end()) {
    return false;
  }
  std::unique_lock<std::mutex> waitlock(latch_);
  LockRequestQueue &lock_queue = lock_table_[rid];
  LockMode lockmode = txn->IsSharedLocked(rid) ? LockMode::SHARED : LockMode::EXCLUSIVE;
  auto iter = lock_queue.request_queue_.begin();
  while(iter != lock_queue.request_queue_.end()) {
    if(iter->txn_id_ == txn->GetTransactionId()) {
      lock_queue.request_queue_.erase(iter);
      break;
    }
    ++ iter;
  }
  // 为什么要在这里改变状态？和之后的notify操作有关么？比如
  if(txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }
  switch(lockmode) {
    case LockMode::SHARED: {
      txn->GetSharedLockSet()->erase(rid);
//      if(lock_queue.request_queue_.empty()) {
        lock_queue.cv_.notify_all();
//      }
      break;
    }
    case LockMode::EXCLUSIVE: {
      assert(lock_queue.request_queue_.size() == 0);
      txn->GetExclusiveLockSet()->erase(rid);
      lock_queue.cv_.notify_all();
      rid_exclusive_[rid] = false;
    }
  }
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  for(const auto &tid : waits_for_[t1]) {
    if(tid == t2) {
      return ;
    }
  }
  waits_for_[t1].push_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  std::vector<txn_id_t> &wait_tids = waits_for_[t1];
  auto iter = wait_tids.begin();
  while(iter != wait_tids.end()) {
    if(*iter == t2) {
      wait_tids.erase(iter);
      break;
    }
    ++ iter;
  }
  if(wait_tids.empty()) {
    waits_for_.erase(t1);
  }
}

bool LockManager::dfs(txn_id_t cur_tid, std::unordered_set<txn_id_t> &Visited, std::unordered_set<txn_id_t> &InCycle) {
  Visited.insert(cur_tid);
  InCycle.insert(cur_tid);
  std::vector<txn_id_t> &wait_tids = waits_for_[cur_tid];
  for(txn_id_t next_tid : wait_tids) {
    if(InCycle.find(next_tid) != InCycle.end()) {
      return true;
    }
    if(Visited.find(next_tid) == Visited.end()) {
      bool cycle = dfs(next_tid, Visited, InCycle);
      if(cycle) {
        return true;
      }
    }
  }
  InCycle.erase(cur_tid);
  return false;
}

bool LockManager::HasCycle(txn_id_t *txn_id) {
  if(waits_for_.empty()) {
    return false;
  }
  std::unordered_set<txn_id_t> Visited;
  std::unordered_set<txn_id_t> InCycle;
  // 首先从最小的txn_id开始dfs，若是没有循环依赖，再从其他未访问的事务开始dfs。
  txn_id_t min_tid = INT_MAX;
  auto iter = waits_for_.begin();
  while(iter != waits_for_.end()) {
    min_tid = std::min(min_tid, iter->first);
    ++ iter;
  }
  iter = waits_for_.begin();
  bool cycle = dfs(min_tid, Visited, InCycle);
  if(!cycle) {
    while(iter != waits_for_.end()) {
      if(Visited.find(iter->first) == Visited.end()) {
        cycle = dfs(iter->first, Visited, InCycle);
      }
      ++ iter;
    }
  }
  if(cycle) {
    txn_id_t newest_tid = INT_MIN;
    for(const txn_id_t &InCycleId : InCycle) {
      newest_tid = std::max<txn_id_t>(newest_tid, InCycleId);
    }
    *txn_id = newest_tid;
    return true;
  }
  return false;
}

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t>> AllEdges;
  auto iter = waits_for_.begin();
  while(iter != waits_for_.end()) {
    for(txn_id_t &r_tid : iter->second) {
      AllEdges.push_back({iter->first, r_tid});
    }
    ++ iter;
  }
  return AllEdges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      // TODO(student): remove the continue and add your cycle detection and abort code here
      txn_id_t tid_to_vic;
      bool cycle = HasCycle(&tid_to_vic);
      if(cycle) {
//         auto edge_list =  GetEdgeList();
//         for(auto edge_pair : edge_list) {
//             std::cout<<edge_pair.first<<" has a pointer to "<<edge_pair.second<<std::endl;
//         }
        Transaction *vic_txn = TransactionManager::GetTransaction(tid_to_vic);
        vic_txn->SetState(TransactionState::ABORTED);
        // throw TransactionAbortException(tid_to_vic, AbortReason::DEADLOCK);
        lock_table_[tid_to_rid_[tid_to_vic]].cv_.notify_all();
      }
    }
  }
}

}  // namespace bustub
