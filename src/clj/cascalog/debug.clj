(ns cascalog.debug)

(def *DEBUG* false)

(defn debug-print [& args]
  (when *DEBUG* (apply println args)))


