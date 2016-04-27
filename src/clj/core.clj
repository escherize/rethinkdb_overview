(ns core
  (:require [rethinkdb.query :as r]
            [schema.core :as s]
            [clojure.string :as str]
            [manifold.stream :as mani]
            [clojure.core.async :as a]))


;; can create connections lazily on demand:
(with-open [conn (r/connect)]
  (r/run (r/db-create "overview") conn))

(with-open [conn (r/connect)]
  (r/run (r/db-drop "overview") conn))



























;;(fabulous place to use mount)
;; r/connect uses the default - can take a config map.
(defonce conn (r/connect :db "cljsyd"))

;; downside of using def conn... is that the db connection
;; needs to be initialized when the file's loaded. i.e. it's eager
;; not lazy.

(defn create-db! [db-name]
  (r/run (r/db-create db-name) conn))

(defn drop-db! [db-name]
  (-> (r/db-drop db-name)
      (r/run conn)))

(defn create-table! [db-name table-name]
  (-> (r/db db-name)
      (r/table-create table-name)
      (r/run conn)))

(comment

  ;; Good for making unit/integration test dbs!
  (time (do (create-db! "hi") (drop-db! "hi")))

  )








;; Let's start assuming db-name is 'cljsyd'

(defn init-db! []
  (create-db! "cljsyd")
  (create-table! "cljsyd" "customers")
  (create-table! "cljsyd" "other_table")
  (create-table! "cljsyd" "lots_of_tables"))

(comment

  (drop-db! "cljsyd")

  (init-db!)

  )








;; inserting

(defn raw-insert! [table-name data]
  "Data is a seq of hashmaps."
  (-> (r/db "cljsyd")
      (r/table table-name)
      (r/insert data)
      (r/run conn)))


















;; Lets put some customers in:
(def Customer {:name s/Str :age s/Int})

(s/defn insert-customer! [customer :- Customer]
  (raw-insert! "customers" (s/validate Customer customer)))

(comment

  (insert-customer! {:name "Leo" :age 30})
  (insert-customer! {:name "Mike" :age 35})
  (insert-customer! {:naxe "RoundPeg" :age 30})

  )


























(def words
  (memoize
   (fn [] (str/split-lines
           (slurp "/usr/share/dict/words")))))

(defn rand-customer []
  {:age (rand-nth (range 18 99))
   :name (-> (words) rand-nth str/capitalize)})

(defn insert-random-customer! []
  (insert-customer! (rand-customer)))

(insert-random-customer!)



























(dotimes [n 100] (insert-random-customer!))


































;; find everyone who's older than 95.






(comment

  (-> (r/db "cljsyd")
      (r/table "customers")
      ;; Filter the table (one would normally use an index for that).
      (r/filter #(< (:age %) 95))
      ;; Update the books for all authors matching the above filter by appending a new title to the array field :books.
      (r/get-field :name)
      (r/run conn))
  )
