;;tests on an AMD EPYC hpc7a.96xlarge
(ns parallelmyth.core
  (:require [ham-fisted.api :as hf])
  (:import [java.util.concurrent ConcurrentHashMap]
           [java.util.function Function]))



(defn work [^long n]
  (loop [i 0]
    (if (< i n)
      (recur (unchecked-inc i))
      i)))

;; parallelmyth.core> (time (count (vec  (hf/pmap work (repeat 96 10000000000)))))
;; "Elapsed time: 2749.10119 msecs"
;; 96
;; parallelmyth.core> (time (count (vec  (hf/pmap work (repeat 96 10000000000)))))
;; "Elapsed time: 2735.840836 msecs"
;; 96
;; parallelmyth.core> (time (count (vec  (hf/pmap work (repeat 1 10000000000)))))
;; "Elapsed time: 2713.688218 msecs"


(defn vec-n [f & {:keys [n k]}]
  (vec  (hf/pmap f (repeat n k))))

(defn time-n [f & {:keys [n k]}]
  (time (count (vec-n f :n n :k k))))

(defn map-work [^long n]
  (loop [i 0
         init {}]
    (if (< i n)
      (recur (unchecked-inc i) (assoc init :i i))
      init)))

;;jdk8
;;parallelmyth.core> (vec-n map-work :n 1 :k 100000000)
;;[{:i 99999999}]

;;parallelmyth.core> (time-n map-work :n 96 :k 100000000)
;;"Elapsed time: 9498.673168 msecs"


;;jdk17 - default gc.
;; parallelmyth.core> (time-n map-work :n 1 :k 100000000)
;; "Elapsed time: 1349.456787 msecs"
;; 1
;; parallelmyth.core> (time-n map-work :n 96 :k 100000000)
;; "Elapsed time: 9438.664164 msecs"
;; 96

;;graalvm 20.0.2
;; parallelmyth.core> (time-n map-work :n 1 :k 100000000)
;; "Elapsed time: 2006.764811 msecs"
;; 1
;; parallelmyth.core> (time-n map-work :n 96 :k 100000000)
;; "Elapsed time: 10928.452581 msecs"
;; 96

(defn map-work-muthf [^long n]
  (loop [i 0
         ^java.util.Map
         init (hf/mut-map)]
    (if (< i n)
      (recur (unchecked-inc i) (doto init (.put :i i)))
      (hf/persistent! init))))



(defn map-work-muthfint [^long n]
  (loop [i 0
         ^java.util.Map
         init (hf/mut-map)]
    (if (< i n)
      (recur (unchecked-inc i) (doto init (.put 0 i)))
      (hf/persistent! init))))

(defn map-work-muthfjint [^long n]
  (loop [i 0
         ^java.util.Map
         init (hf/java-hashmap)]
    (if (< i n)
      (recur (unchecked-inc i) (doto init (.put 0 i)))
      (hf/persistent! init))))


;;jdk17 defaultgc 
;; parallelmyth.core> (time-n map-work-muthfjint  :n 1 :k 100000000)
;; "Elapsed time: 365.818796 msecs"
;; 1
;; parallelmyth.core> (time-n map-work-muthfjint  :n 96 :k 100000000)
;; "Elapsed time: 10251.174571 msecs"
;; 96


;;graalvm 20.0.2
;; parallelmyth.core> (time-n map-work-muthfjint  :n 1 :k 100000000)
;; "Elapsed time: 794.895727 msecs"
;; 1
;; parallelmyth.core> (time-n map-work-muthfjint  :n 96 :k 100000000)
;; "Elapsed time: 8256.160891 msecs"
;; 96

(defn map-work-muthfjconst [^long n]
  (loop [i 0
         ^java.util.Map
         init (hf/java-hashmap)]
    (if (< i n)
      (recur (unchecked-inc i) (doto init (.put 0 1)))
      (hf/persistent! init))))


(defn map-work-muthfjkey [^long n]
  (loop [i 0
         ^java.util.Map
         init (hf/java-hashmap)]
    (if (< i n)
      (recur (unchecked-inc i) (doto init (.put 0 :hello)))
      (hf/persistent! init))))

(def global (atom 0))
(defn map-work-muthfjkeyatom [^long n]
  (loop [i 0
         ^java.util.Map
         init (hf/java-hashmap)]
    (if (< i n)
      (recur (unchecked-inc i) (doto init (.put 0 (deref global))))
      (hf/persistent! init))))


(defn Func ^Function [f]
  (reify Function
    (apply [this v] (f v))))

(defn get-binding! [^ConcurrentHashMap ctx k ^Function f]
  (.computeIfAbsent ctx k f))

(defmacro pseudo-thread-local [[id init] & body]
  `(let [ctx#        (ConcurrentHashMap.)
         init-fn#    (Func (fn [k#] ~init))
         ~id         (reify clojure.lang.IDeref
                       (deref [this#]
                         (get-binding! ctx# (.getId (Thread/currentThread)) init-fn#)))]
     ~@body))

#_
(pseudo-thread-local [gen (java.util.Random.)]
  (defn rand!
    (^double []
     (.nextDouble ^java.util.Random @gen))
    (^double [n]
     (* n (rand!))))
  (defn rand-int! [n]
    (int (rand! n))))


;;slow, contended.
(defn map-work-muthfjrandvec [^long n]
  (loop [i 0
         ^java.util.Map
         init (hf/java-hashmap)]
    (if (< i n)
      (let [entry [(rand-int 10) (rand-int 10)]]
        (recur (unchecked-inc i) (doto init (.put 0 entry))))
      (hf/persistent! init))))


(def ^:dynamic *rng* (java.util.Random.))

(defn rand!
  (^double []
   (.nextDouble ^java.util.concurrent.ThreadLocalRandom
      (java.util.concurrent.ThreadLocalRandom/current)))
  (^double [n]
   (* n (rand!))))

(defn rand-int! ^long [n]
  (int (rand! n)))

;;can hit up to 32x semi consistently with 32 threads.
(defn map-work-muthfjrandvec! [^long n]
  (loop [i 0
         ^java.util.Map
         init (hf/java-hashmap)]
    (if (< i n)
      (let [entry [(rand-int! 10)
                   (rand-int! 10)]]
        (recur (unchecked-inc i) (doto init (.put 0 entry))))
      (hf/persistent! init))))
