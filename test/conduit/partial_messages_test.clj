(ns conduit.partial-messages-test
  (:require [conduit.partial-messages :refer [perhaps-send-partial wrap-parser-result]]
            [clojure.test :refer [testing is deftest]]))

(deftest partial-message-round-trip-test
  (let [parse (wrap-parser-result {})
        received (atom [])
        sender  (fn [message]
                  (perhaps-send-partial message
                                        (+ 100 (rand-int 100))
                                        {}
                                        (fn [m] (swap! received conj m))))
        test-data [{:a 0 :b (repeat 1000 "HELLO")}
                   {:unicode (repeat 100000 "日常まとめ 2014年の振り返り 2014年12月31日 コメントをどうぞ スキル 英語：うーん。。。 マネージメント：ぼちぼち 技術：プログラミング、欠点克服 内面：ぼちぼち アウトプット ")}]
        result (delay (map parse @received))]
    (doseq [datum test-data]
      (sender datum))
    (is (not= test-data @result))
    (is (= (remove #{:partial/consumed} @result)
           test-data))))

(deftest partial-not-applied-test
  (let [parse (wrap-parser-result {})
        received (atom [])
        sender (fn [message]
                 (perhaps-send-partial message
                                       Integer/MAX_VALUE
                                       {}
                                       (partial swap! received conj)))
        test-data (repeat 1000 {:unicode (repeat 100 "日常まとめ 2014年の振り返り 2014年12月31日 コメントをどうぞ スキル 英語：うーん。。。 マネージメント：ぼちぼち 技術：プログラミング、欠点克服 内面：ぼちぼち アウトプット ")})]
    (doseq [datum test-data]
      (sender datum))
    (is (= test-data
           @received
           (map parse @received)))))

(deftest partial-recursive-test
  (letfn [(break-up [messages size counter]
            (let [received (atom [])
                  sender (fn [message]
                           (perhaps-send-partial message
                                                 size
                                                 {}
                                                 (fn [m]
                                                   (swap! received conj m))))
                  new-size (Math/ceil (/ size 1.1))]
              (doseq [datum messages]
                (sender datum))
              (println (count messages) 'parts 'encoded)
              (if (< new-size 75)
                @received
                (recur @received new-size
                       (+ counter (count messages))))))]
    (let [messages (repeat 100 {:a [1 2 3] :b (range 10) :c  (repeat 100 "aメント：ぼちぼち 技術：プログラミング、欠点克服 内面：")})
          parse (wrap-parser-result {})
          result (map parse (break-up messages 100 0))]
      (println 'decoding (count result) 'parts)
      (is (= messages
             (remove #{:partial/consumed} result))))))

(def bad "ãªãã®ï¼ï¼·ï½ï¼´ã»ããªãå¾¹å¯ãã§ãããçã¡ããããã¡ããå¥½ãã§ããç¡è¨ãã©ã­ã¼OKãæ°ç´ãã§ãã©ã­ãä¼ºã£ã¦ã¾ãã")
