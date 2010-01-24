(ns fleetdb.helper)

;; find-options
(defn offset [n]
  {:offset n})

(defn limit [n]
  {:limit n})

(defn only [& attr-names]
  (if (= 1 (count attr-names))
    {:only (first attr-names)}
    {:only (apply vector attr-names)}))

(defn asc [attr]
  [attr "asc"])

(defn desc [attr]
  [attr "desc"])

(defn order [& attrs]
  (if (= 1 (count attrs))
    {:order (first attrs)}
    {:order (apply vector attrs)}))

(defmacro where [criteria]
  `{:where (where-criteria ~criteria)})

(defmacro where-criteria [[c & args]]
  (let [c (str c)]
    (condp contains? c
      #{"or" "and"}
           `[~c ~@(map #(list 'where-criteria %) args)]
      #{"=" "!=" "<" "<=" ">" ">=" "in" "><" "><=" ">=<" ">=<="}
           `(vector ~c ~@args)
      `(throw (Exception. (str "Unknown where criteria '" ~c "'"))))))

(defn join-options [options]
  (into {} options))

;; Queries

(defn ping []
  [:ping])

(defn select
  ([collection]
      [:select collection])
  ([collection & find-options]
      [:select collection (join-options find-options)]))

(defn fleetdb-count
  ([collection]
      [:count collection])
  ([collection & find-options]
      [:count collection (join-options find-options)]))

(defn insert [collection & records]
  (if (= 1 (count records))
    [:insert collection (first records)]
    [:insert collection (apply vector records)]))

(defn update
  ([collection update-map]
      [:update collection update-map])
  ([collection update-map & find-options]
      [:update collection update-map (join-options find-options)]))

(defn delete
  ([collection]
      [:delete collection])
  ([collection & find-options]
      [:delete collection (join-options find-options)]))

(defn create-index [collection index-spec]
  [:create-index collection index-spec])

(defn drop-index [collection index-spec]
  [:drop-index collection index-spec])

(defmacro multi-read [& queries]
  `[:multi-read [~@queries]])

(defmacro multi-write [& queries]
  `[:multi-write [~@queries]])

(defn checked-write [read-query expected-read-result write-query]
  [:checked-write read-query expected-read-result write-query])

(defn explain [query]
  [:explain query])

(defn list-collections []
  [:list-collections])

(defn list-indexes [collection]
  [:list-indexes collection])

(defn compact []
  [:compact])
