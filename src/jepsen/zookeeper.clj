(ns jepsen.zookeeper
  (:gen-class)
  (:require [avout.core :as avout]
             [clojure.tools.logging :refer :all]
             [clojure.java.io :as io]
             [clojure.string :as str]
             [knossos.model :as model]
             [jepsen [cli :as cli]
                     [checker :as checker]
                     [tests :as tests]
                     [db :as db]
                     [control :as c]
                     [tests :as tests]
                     [generator :as gen]
                     [nemesis :as nemesis]
                     [client :as client]
                     [util :as util :refer [timeout]]]
             [jepsen.os.debian :as debian]
             [jepsen.checker.timeline :as timeline]))


;!lein run -- test --nodes-file ~/nodes --username admin

(defn zk-node-ids
  "Given a test configuration build a map of nodes"
  [test]
  (->> test
       :nodes
       (map-indexed (fn [i node] [node i]))
       (into {})))

(defn zk-node-id
  "Get the id of the given node"
  [test node]
  ((zk-node-ids test) node))

(defn zoo-cfg-servers
  "Create a config fragmeng in zoo.cfg"
  [test]
  (->> (zk-node-ids test)
       (map (fn [[node id]]
              (str "server." id "=" (name node) ":2888:3888")))
       (str/join "\n")))

(defn db
  "A Zookeeper DB at the given revision"
  [version]
  (reify db/DB
    (setup! [_ test node]
      (c/su
        (info "Setting up ZK on" node)
        (debian/install {:zookeeper version
                         :zookeeper-bin version
                         :zookeeperd version})
        (c/exec :echo (zk-node-id test node) :> "/etc/zookeeper/conf/myid")
        (c/exec :echo (str (slurp (io/resource "zoo.cfg"))
                                               "\n"
                                               (zoo-cfg-servers test))
                                  :> "/etc/zookeeper/conf/zoo.cfg")
        (info node "starting ZK")
        (c/exec :service :zookeeper :restart)
        (Thread/sleep 5000)
        (info node "started ZK")))

    (teardown! [_ test node]
      (info "Tearing down ZK on" node)
      (c/su
        (c/exec :service :zookeeper :stop)
        (c/exec :rm :-rf
                (c/lit "/var/ib/zookeeper/version-*")
                (c/lit "/var/log/zookeeper/*"))))
    db/LogFiles
    (log-files [db test node]
      ["/var/log/zookeeper/zookeeper.log"])))

(defn r [_ _] {:type :invoke
                :f :read
                :value nil})

(defn w [_ _] {:type :invoke
                :f :write
                :value (rand-int 5)})

(defn cas [_ _] {:type :invoke
                  :f :cas
                  :value [(rand-int 5) (rand-int 5)]})

(defn client
  "A client for a single compare-and-set register"
  [conn a]
  (reify client/Client
    (setup! [_ test node]
      (let [conn (avout/connect (name node))
            a (avout/zk-atom conn "/jepsen" 0)]
        (client conn a)))

    (invoke! [this test op]
      (timeout 5000 (assoc op :type :info, :error :timeout)
               (case (:f op)
                 :read (assoc op :type :ok, :value @a)
                 :write (do
                          (avout/reset!! a (:value op))
                          (assoc op :type :ok))
                 :cas (let [[value value'] (:value op)
                            type           (atom :fail)]
                        (avout/swap!! a (fn [current]
                                          (if (= current value)
                                            (do
                                              (reset! type :ok)
                                              value')
                                            (do
                                              (reset! type :fail)
                                              current))))
                        (assoc op :type @type)))))

  (teardown! [_ test]
             (.close conn))))


(defn zk-test
  "generate a test map from arguments"
  [opts]
  (merge
    tests/noop-test
    {:nodes (:nodes opts)
     :ssh (:ssh opts)
     :os debian/os
     :name "zookeper"
     :db (db "3.4.5+dfsg-2")
     :client (client nil nil)
     :nemesis (nemesis/partition-random-halves)
     :generator (->> (gen/mix [r w cas])
                     (gen/stagger 1)
                     (gen/nemesis
                       (gen/seq (cycle [(gen/sleep 5)
                                        {:type :info :f :start}
                                        (gen/sleep 5)
                                        {:type :info :f :stop}])))

                     (gen/time-limit 15))
     :model (model/cas-register 0)
     :checker (checker/compose {:perf (checker/perf)
                                 :linear checker/linearizable
                                 :html (timeline/html)})}))

(defn -main
  "Handle command line arguments"
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn zk-test})
                    (cli/serve-cmd))
            args))
