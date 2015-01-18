(ns readwritelock-scratch.scratch
  (:import (java.util.concurrent ArrayBlockingQueue)
           (java.util.concurrent.locks ReentrantReadWriteLock))
  (:require [clojure.tools.logging :as log]
            [criterium.core :as criterium]))

(defn data-queue
  []
  (let [q (ArrayBlockingQueue. 10)]
    (dotimes [i 10]
      (log/debug "Adding to queue:" i)
      (.put q i))
    q))

(defn agent-finished
  [{:keys [id num-agents done-count done-promise]}]
  (log/debug "Agent" id "complete.")
  (let [num-done (swap! done-count inc)]
    (log/debug "Updated done count to:" num-done)
    (when (= num-agents num-done)
      (log/debug "All agents complete, delivering done promise")
      (deliver done-promise true))))

(defn my-agent-error
  [a e]
  (log/debug "Agent" (:id @a) "had an error!" e a)
  (agent-finished @a))

(defn agents
  [n done-promise done-count]
  (mapv (fn [id]
          (log/debug "Creating agent:" id)
          (let [a (agent {:id id
                          :done-count done-count
                          :done-promise done-promise
                          :num-agents n})]
            (set-error-handler! a my-agent-error)
            #_(add-watch a :done-count-watch
                       (fn [k r old-state new-state]
                         (agent-finished new-state)))
            a))
        (range n)))

(defn borrow-and-return
  [q]
  (let [x (.take q)]
    (.put q x)
    x))

(defn borrow-with-lock-and-return
  [l q]
  (try
    (.. l readLock lock)
    (log/debug "Read lock acquired; lock count:" (.getReadLockCount l))
    (borrow-and-return q)
    (finally
      (.. l readLock unlock)))
  (log/debug "Released read lock; lock count:" (.getReadLockCount l)))

(defn do-borrow
  [borrow-fn num-threads borrows-per-thread]
  (let [q (data-queue)
        done-promise (promise)
        done-count (atom 0)
        as (agents num-threads done-promise done-count)
        borrows-fn (fn [a]
                     (log/debug "Starting agent:" (:id a))
                     (dotimes [i borrows-per-thread]
                       (log/debug "Agent" (:id a) "borrowing #" i)
                       (let [x (borrow-fn q)]
                         (log/debug "Agent" (:id a) "borrow #" i "returned" x)))
                     (agent-finished a)
                     a)]
    (doseq [a as]
      (send-off a borrows-fn))
    (log/debug "Waiting for agents to complete")
    @done-promise
    (log/debug "All agents completed, checking for errors")
    (doseq [a as]
      (if-let [e (agent-error a)]
        (throw e)))
    (log/debug "All agents completed successfully.")))

(defn do-bench-borrow
  [borrow-fn num-threads borrows-per-thread]
  (log/info
    "Mean:"
    (-> (criterium/benchmark
          (do-borrow borrow-fn num-threads borrows-per-thread)
          {:verbose true})
        :mean
        first
        (* 1000))
    "ms"))

(defn borrow
  [num-threads borrows-per-thread]
  (do-borrow borrow-and-return num-threads borrows-per-thread))

(defn bench-borrow
  [num-threads borrows-per-thread]
  (do-bench-borrow
    borrow-and-return
    num-threads
    borrows-per-thread))

(defn borrow-with-lock
  [num-threads borrows-per-thread fair]
  (do-borrow (partial borrow-with-lock-and-return (ReentrantReadWriteLock. fair))
             num-threads
             borrows-per-thread))

(defn bench-borrow-with-lock
  [num-threads borrows-per-thread fair]
  (do-bench-borrow
    (partial borrow-with-lock-and-return (ReentrantReadWriteLock. fair))
    num-threads
    borrows-per-thread))