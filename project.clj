(defproject liberator-tutorial "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1" :debian nil]
                 [org.clojure/algo.generic "0.1.1" :debian nil]
                 [incanter/incanter-core "1.5.4" :debian nil]
                 [incanter/incanter-charts "1.5.4" :debain nil]
                 [liberator "0.9.0" :debian nil]
                 [compojure "1.1.5" :debian nil]
                 [ring/ring-jetty-adapter "1.2.0" :debian nil]
                 [http-kit "2.1.12" :debian nil]
                 [org.clojure/java.jdbc "0.2.3" :debian nil]
                 [korma "0.3.0-RC5" :debian nil]
                 [org.xerial/sqlite-jdbc "3.7.2" :debian [libsqlitejdbc.so] ]
                 [org.clojure/data.xml "0.0.7" :debian nil]
                 [org.clojure/data.json "0.2.3" :debian nil]
                 [ring-mock "0.1.5" :scope "test" :debian nil]

                 [clj-message-digest "1.0.0" :debian nil]

                 [couchbase-clj "0.1.2"]
                 ]
  :target-path "target"
  :plugins [[lein-debian "0.14.0-SNAPSHOT"]]

  :repositories [["snapshots" "file:/home/pdeschacht/.m2/local/"]]

  :debian {:dependencies []}
)
