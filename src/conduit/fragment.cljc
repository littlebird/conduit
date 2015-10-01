(ns conduit.fragment
  (:require [clojure.string :as string]
            #?(:clj
               [clojure.data.codec.base64 :as b64]
               :cljs
               [goog.crypt.base64 :as b64])
            #?(:clj
               [clojure.edn :as rd]
               :cljs
               [cljs.reader :as rd])
            [schema.core :as schema]))

(defonce readable (atom false))

(def b64-encode
  #?(:clj
     #(String. (b64/encode (.getBytes %)))
     :cljs
     b64/encodeString))

(def b64-decode
  #?(:clj
     #(String. (b64/decode (.getBytes %)))
     :cljs
     b64/decodeString))

(defn parse
  "Creates a map matching the FragmentStateSchema out of a fragment."
  [fragment]
  (let [[_ base opts] (re-matches #"^#/([^{?]*)(\{.*\})*" fragment)
        [base opt-string] (if base
                            [base opts]
                            (let [[_ base opts] (re-matches #"^#/([^\?]*)(\?.*)" fragment)]
                              (when (> (count opts) 1)
                                [base (b64-decode (subs opts 1))])))
        opt-string (or (not-empty opt-string) "{}")
        opts (rd/read-string opt-string)]
    (assoc opts :base base)))

(defn construct
  [fragment]
  {:pre [(map? fragment)]}
  (let [base (:base fragment)
        opts (not-empty (dissoc fragment :base))
        opts (when opts
                  (if @readable
                    (pr-str opts)
                    (str "?" (b64-encode (pr-str opts)))))]
    (str "#/"
         base
         opts)))

(defn update-fragment-impl
  [f get-f set-f]
  (set-f (construct (f (parse (get-f))))))

(defn assoc-impl
  "associate some data to the fragment"
  [impl & kvs]
  (impl (fn [f] (apply assoc f kvs))))

(defn dissoc-impl
  [impl & ks]
  (if ks
    (impl (fn [f] (apply dissoc f ks)))
    (impl (fn [f] (select-keys f [:base])))))

(defonce readable (atom false))

(defn get-fragment-raw
  []
  #?(:cljs
     (-> js/window .-location .-hash)))

(defn set-fragment-raw
  [h]
  #?(:cljs
     (-> js/window .-location .-hash (set! h))))

(def FragmentStateSchema
  {(schema/required-key :base) schema/Str
   schema/Keyword schema/Any})

(schema/defn get-fragment :- FragmentStateSchema
  "Returns a hash map, :base will be the top level of the fragment,
  other data will be under keyword keys"
  []
  #?(:cljs
     (parse (get-fragment-raw))))

(schema/defn set-fragment :- nil
  "Takes a hash-map, must contain the key :base with a string value,
  other keyword keys and edn* values are allowed"
  [h :- FragmentStateSchema]
  #?(:cljs
     (set-fragment-raw (construct h))))

(defn update-fragment
  "f should take and return a hash-map
  set the top level location via :base
  control any ancillary data via other keywords,
  values allowed are edn*
  *(without reader tags)"
  [f]
  (update-fragment-impl f get-fragment-raw set-fragment-raw))

(def assoc-fragment
  (partial assoc-impl update-fragment))

(def dissoc-fragment
  (partial dissoc-impl update-fragment))

(defn href
  [fragment]
  (str "/" (construct fragment)))
