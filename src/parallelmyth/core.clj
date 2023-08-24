;;tests on an AMD EPYC hpc7a.96xlarge
(ns parallelmyth.core
  (:require [ham-fisted.api :as hf]))


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

;;parallelmyth.core> (vec-n map-work :n 1 :k 100000000)
;;[{:i 99999999}]

;;parallelmyth.core> (time-n map-work :n 96 :k 100000000)
;;"Elapsed time: 9498.673168 msecs"

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
