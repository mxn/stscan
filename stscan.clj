;(add-classpath "file:///C:/MySoft/clojure/clojure-contrib.jar")
(add-classpath "file:///home/novosma/Stuff/jtsapi/IBJts/jtsclient.jar")
(add-classpath "file:///home/novosma/Stuff/jtsapi/IBJts/dst/ib_tests.jar")


(ns dnld
;;   (:require clojure.contrib.duck-streams)
;;   (:require clojure.contrib.pprint)
  (:require [clojure.contrib [duck-streams :as ds]])
  (:require [clojure.contrib [pprint :as pp]])
  )

(defn set-proxy [proxy-url]
  (.put (System/getProperties) "http.proxyHost" proxy-url))

(import 
  '(java.lang StringBuilder)
  '(java.net URL)
  '(java.io StringWriter FileReader BufferedReader InputStreamReader File)  
  '(java.util Date Calendar)
  '(java.text SimpleDateFormat)
  '(java.util.regex Pattern)
  '(com.ib.client Order Contract EClientSocket)
  '(tests ApiCallback)
)

(def *df* (new java.text.SimpleDateFormat "yyyy-MM-dd"))

(def *rt-price-gg-re* #"<span class=\"pr\"><span id.*?>(\d+\.\d+)<")
(def *rt-open-pr-gg-re* (. Pattern compile "class=key>Open:.*?class=val>(\\d+\\.\\d*).*?</td>" (. Pattern MULTILINE)))
(def *rt-time-gg-re* (. Pattern compile "Real-time:.*?(\\d+:\\d+[AP]M) EDT</span" (. Pattern MULTILINE)))
; #"<span style=\"white-space:normal;\" id=.*?(\d+:\d+[AP]M) EDT</span")


(def *num-workers* 7)

(defn parse-date [dt]
  (cond 
   (string? dt) (.parse *df* dt)
   (number? dt) (new Date (long dt))
   true    dt))

(defn calendar [& [dt]]
  (let [c (Calendar/getInstance)]
  (.setTime c (if dt (parse-date dt) (new Date))) c))

(defn format-date [frmt-str & [date]]
  (.format (new SimpleDateFormat frmt-str)  (if date date (new Date))))

(defn mk-quote-url [stock-sym & [start-date end-date]]
  (let [sym (.toUpperCase (str stock-sym))
	start-date-cal (if start-date 
			 (calendar start-date)
			 (doto (calendar) (.add Calendar/MONTH -1)))
	end-date-cal (calendar end-date)]
    (str  
		 "http://ichart.finance.yahoo.com/table.csv?s=" sym    
		 "&a=" (pp/cl-format nil "~2,'0d" (.get start-date-cal Calendar/MONTH))
		 "&b=" (pp/cl-format nil "~2,'0d" (.get start-date-cal Calendar/DATE))
		 "&c=" (pp/cl-format nil "~a" (.get start-date-cal Calendar/YEAR))
		 "&d=" (pp/cl-format nil "~2,'0d" (.get end-date-cal Calendar/MONTH))
		 "&e=" (pp/cl-format nil "~2,'0d"  (.get end-date-cal Calendar/DATE))
		 "&f=" (pp/cl-format nil "~a" (.get end-date-cal Calendar/YEAR))
		 "&g=d&ignore=.csv")))

(defn get-stk-file [stock-sym dst-dir & [start-date end-date]]
  (let [dst-file (new java.io.File dst-dir (str (.toUpperCase (str stock-sym)) ".csv" ))]
    (ds/copy (.openStream (new URL (mk-quote-url stock-sym start-date end-date))) dst-file)))

(defmacro ignore-err [body]
  `(try ~body (catch Throwable e#)))

 


(defn split-for-n [v n]
  (let [delta (Math/ceil (/ (count v) n ))
	vlen (count v)]
  (map #(subvec v (* delta %) (min vlen (* delta (+ 1 %)))) (range (min n (Math/ceil (/ vlen delta)))))))

(defn vector-pmap [f v]
  (let [n *num-workers*        
        agents (map agent (split-for-n v n))]
    (doseq [a agents]
      (send a #(doall (map f %))))
    (apply await agents)
    (into [] (apply concat (map deref agents)))))

;(defn reas

;; (defn parse-csv-read [reader]  
  
;;   (line-seq 

(defn fetch-url
  "Return the web page as a string."
  [address]
  (let [url (URL. address)]
    (with-open [stream (. url (openStream))]
      (let [buf (BufferedReader. (InputStreamReader. stream))]
        (apply str (line-seq buf))))))

(defn get-rt-quote [sym]
  (read-string (second (re-find *rt-price-gg-re* (fetch-url (str "http://www.google.com/finance?q=" (.toUpperCase (str sym))))))))

(defn get-rt-quote-ext [sym]
  (let [page-str (fetch-url (str "http://www.google.com/finance?q=" (.toUpperCase (str sym))))]
    {:rt-price 
     (read-string (second (re-find *rt-price-gg-re* page-str)))
     :time-str
     (second (re-find *rt-time-gg-re* page-str))
     :open 
     (read-string (second (re-find *rt-open-pr-gg-re* page-str)))}))



(defn mk-kw [string]
  (symbol (str ":" string)))
  

(defn parse-csv-read [reader]  
  (let  [flds  (atom [])
	 content-? (atom false)
	 content (atom [])]
    (doseq [l (ds/read-lines reader)]
      (if @content-?
	(let [hmap (atom (hash-map))]
	  (doall (map 
		  (fn [fld-name fld-val]
		    (swap! hmap assoc fld-name fld-val))	  
		      @flds
		      (seq (.split l ","))))
	  (swap! content conj @hmap))
		  

	(do 
	  (doseq [fld (.split l ",")]
	    (swap! flds conj (mk-kw (.toLowerCase (.replace fld \ \- )))))
	  (swap! content-? (fn [_] true)))))
    @content))

(defstruct st-bar :date :open :high :low :close :volume :adj-close)

(defn parse-yhoo-stock-csv [reader]
    (let  [ content (atom [])
	   fields [:date :open :high :low :close :volume :adj-close]
]
      (doseq [l (ds/read-lines reader)]
	(when-not (.startsWith l "Date")
	  (let [stbar (atom (struct st-bar))]	    	   
	    (doall (map  
	     (fn [k v]
	       (swap! stbar assoc k (if (= k :date) (parse-date v) (read-string v))))	   
	     fields
	     (.split l ",")
	     ))
	    (swap! content conj @stbar))))
      @content))

(defn avg [seq]
  (/ (reduce + 0 seq) (count seq)))

(defn median [seq & [comparator]]
  (when (> (count seq) 0)
    (let [sorted (if comparator (sort seq comparator) (sort seq))
	  cnt (int (/ (- (count sorted) 1) 2))]
      (nth sorted cnt ))))
    

(defn good-st-data? [file]
  (let [bars (parse-yhoo-stock-csv file)]
  (and (> (:close (first bars)) 8.0)
       (> (avg (map #(* (:close %) (:volume %)) bars)) 2000000))))



(defn oc-med [hist-data]
  (let [ocs
	(doall (map #(let [arange (- (:high %)  (:low %))] (if (> arange 0)  (Math/abs (/ (- (:close %) (:open %)) arange)) 0)) hist-data))]
    (median ocs)))

(def *data-dir* "/home/novosma/Stuff/clojure/data/")
(def *sym-file* "/home/novosma/Stuff/clojure/good-syms.txt")
;(def *sym-file* "/home/novosma/Stuff/clojure/good-syms-tmp.txt")
(def *stk-analysis-file-prefix* "/home/novosma/Stuff/clojure/analysis")


(defn stk-file-analysis [file]
  (let 
      [sym (second (re-find  #"([a-zA-Z]+).csv" (.getName file)))
       hist-data (parse-yhoo-stock-csv file)
       oc (oc-med hist-data)
       last-bar (first hist-data)
       last-date (format-date "yyyyMMdd" (:date last-bar))       
       last-close (:close last-bar)
       last-down-shadow-rat (let [brange (- (:high last-bar) (:low last-bar))]
			      (if (= range 0) 0
				 (/  (- (min (:open last-bar) (:close last-bar))
				     (:low last-bar)) brange)))				  
       liquidity (* (:close (first hist-data)) (:volume (first hist-data)))

       avg-range (avg (map  #(- (:high %) (:low %)) hist-data))]
    {:sym sym :oc-median oc :avg-range avg-range :last-date last-date :last-close last-close :liquidity liquidity 
     :last-dn-sh-rat last-down-shadow-rat}))

(def *min-liquidity* 2000000)
(def *min-range-to-close-ratio* 0.02)
(def *min-med-oc* 0.5)
(def *min-dn-sh-rat* 0.3)
(def *delta-to-avg-range-rat* 0.4) 
(def *bo-rat* 0.3) 

(defn filter-for-osetup []
  (let [an-records (map read-string 
			(ds/read-lines 
			 (str *stk-analysis-file-prefix* "-" (format-date "yyyyMMdd") ".txt"))) 
	greatest-date (last (sort (map :last-date an-records)))]
    (filter #(and (= (:last-date %) greatest-date) 
		  (> (:liquidity %) *min-liquidity*)		  
		  (> (/ (:avg-range %)  (:last-close %))  *min-range-to-close-ratio*)
		  (> (:oc-median %) *min-med-oc*)
		  (> (:last-dn-sh-rat %) *min-dn-sh-rat*)) an-records)))

(def *state-dir* "/home/novosma/Stuff/clojure/")




(def +na+ :NA)

(defn calc-or-setup [an-record]  
  "Check RT quote and either return the or level or nil or +na+ if the rt quote is unavailable"
  (let [g-rt-rec (get-rt-quote-ext (:sym an-record))
	delta  (* *delta-to-avg-range-rat* (:avg-range an-record))
		
	gap-level (- (:last-close an-record) delta)]        
      (when (and g-rt-rec (:open g-rt-rec))
	(if (< (:open g-rt-rec) gap-level)
	  (+ (:open g-rt-rec) (* *bo-rat* (:avg-range an-record)))
	   false))))

(defn check-opens []
  (vector-pmap (fn [an] {:el-price (calc-or-setup an) :an-record an})  (vec (filter-for-osetup))))

;; (defn  [

;; (defn place-orders []
;;   (let [recs (check-opens)
;; 	rec-for-orders (doall (filter #(number? (:el-price %)) @*opens*))
;; 	rec-to-check  (doall (filter #(= nil (:el-price %)) @*opens*))]
;; ;    (ds/write 
    
    
    
	    
(defn stscan-analysis []
  ;fisrt clear
  (doseq [f (filter #(.endsWith (.getName %) "csv") (.listFiles (new File *data-dir*)))]
    (.delete f))
  ; second download and save
  (doall (vector-pmap  
	  #(ignore-err (get-stk-file  % *data-dir*))
	  (vec  (ds/read-lines *sym-file*))))
  ; then analysis
  (ds/write-lines (str *stk-analysis-file-prefix* "-" (format-date "yyyyMMdd") ".txt")
		  (map stk-file-analysis
		       (.listFiles (new File *data-dir*)))))
	  
;;(doall (filter #(= (first %) true) (map (fn [an] (vector (calc-or-setup an) an))  (filter-for-osetup))))    

;;;;;;

(def *tbl-req* (atom {}))
(def *tbl-res* (atom {}))

(def *ids* (atom 1))
(def *time-out-s* 30)

(defn get-id [] (swap! *ids* inc))

 
(defmacro fill-jfields [obj field-vals]
  (let [oname (gensym "obj")]
    `(let [~oname ~obj]
       ~@(map  (fn [pair] `(set! (.  ~oname ~(first pair)) ~(second pair)  )) (partition 2 field-vals))
       ~oname)))



(defn response-completed [reqId]
  (when-let [latch (get @*tbl-req* reqId)]    
    (.countDown latch)))

(defn complete-req-res [req-id] 
  (let [res (get @*tbl-res* req-id)]
    (swap! *tbl-req* dissoc req-id)
    (swap! *tbl-res* dissoc req-id)
    res))

(defn sync-bridge [id fn & [n]]
  (let [n (if n n 1)]
    (swap! *tbl-req* assoc id (new java.util.concurrent.CountDownLatch n))
    (fn)
    (.await (get  @*tbl-req* id) *time-out-s* java.util.concurrent.TimeUnit/SECONDS)
    (complete-req-res  id)))


(defn on-hist-data [reqId date open high low close volume count WAP hasGaps]
  (if (< open 0)
     (response-completed reqId)
    (let [res (get @*tbl-res* reqId)]
      (if res
      (let [new-v  (conj 
		    res
		    {:reqId reqId :date date :open open :high high :low  low :close close :volume volume :count count
		       :WAP WAP :hasGaps hasGaps})]
	;(println "res")
	(swap! *tbl-res* assoc reqId new-v))		    
       (swap!  *tbl-res* assoc reqId 
	     [{:reqId reqId :date date :open open :high high :low  low :close close :volume volume :count count
		       :WAP WAP :hasGaps hasGaps}])
))))

(defn on-error [id  errorCode errorString]
  (response-completed id)
  (println (str id " " errorCode " " errorString)))

(def *order-id* (atom nil))

(defn next-order-id [& [id]]
  (if id 
    (swap! *order-id* (fn [_]  (inc id)))
    (swap! *order-id* inc)))

  

(def *eclient* (new EClientSocket 
		    (proxy [ApiCallback] []
			(historicalData [reqId date open high low close volume count WAP hasGaps ]
					( on-hist-data reqId date open high low close volume count WAP hasGaps ))
			(error  [id  errorCode errorString]
				(on-error id  errorCode errorString))
			(nextValidId [ordId] (next-order-id ordId))
)))
				


(def *host* "localhost")
(def *port* 7496)
(def *client* 0)


					
(defn start-session []
 (.eConnect *eclient* *host* *port* *client*))

(defn stop-session []
  (.eDisconnect *eclient*))
    
(defn stk [sym]
  (fill-jfields (new Contract)
		[m_symbol  (.toUpperCase (str sym))
		 m_secType  "STK"
		 m_currency "USD"
		 m_exchange  "SMART"]))

(defn get-hist-data [contract durationStr barSizeStr & endDateTime ]
  (let [req-id (get-id)
	end-date (if endDateTime (parse-date endDateTime) 
		    (str  (format-date "yyyyMMdd")  " 23:59:59"))]
    (sync-bridge req-id 
		 (fn []
		   (.reqHistoricalData *eclient* 
				       req-id  contract end-date  
				       durationStr barSizeStr (if (= (.m_secType contract) "STK") "TRADES" "MIDPOINT")
							    1 1)))))
  
  
(defn stop [price]
  (fill-jfields (new Order)
		[m_orderType "STP"
		 m_auxPrice price]))

(defn limit [price]
  (fill-jfields (new Order)
		[m_orderType "LMT"
		 m_lmtPrice price]))

(defn mkt-close []
  (fill-jfields (new Order)
		[m_orderType "MKTCLS"]))


(defn stop-limit [stop-price limit-price]
  (fill-jfields (new Order)
		[m_orderType "STPLMT"
		 m_lmtPrice limit-price
		 m_auxPrice stop-price]))

(def *oca-grp* "GPDN")

(defn fill-order-attrs [action  qty order]
  (fill-jfields order
		[m_orderId (next-order-id)
		 m_clientId *client*
		 m_action action
		 m_totalQuantity qty		
		 ]))

(defn close-action [entry-action]
  (condp =  entry-action
	"BUY" "SELL"
	"SELL" "BUY"
	"SSHORT" "BUY"))



(defn bracket-orders [action contract qty eord & [close-orders]]
  (fill-order-attrs action  qty eord)
  (fill-jfields eord [m_transmit false, m_tif "DAY", m_transmit (if close-orders false true)
		       m_ocaGroup *oca-grp*
		      m_ocaType 1])
  (.placeOrder *eclient* (.m_orderId eord) contract eord)
  (let [par-id (.m_orderId eord)]
    (when close-orders      
      (doall (map (fn [cl-ord] 		    
		   ; (println "par: " par-id)
		    (fill-order-attrs  (close-action action)  qty cl-ord)
		    ;(println "order-attrs is filled: " par-id)
		    (fill-jfields cl-ord [m_parentId par-id, 
					  m_tif "GTC",
					  m_transmit true])
		    (.placeOrder *eclient*  (.m_orderId cl-ord) contract cl-ord))
	     close-orders
	     )))))
    
(defn buy-bracket [contract qty eord & [close-orders]]
    (bracket-orders "BUY" contract qty eord  close-orders))


(defn sell-bracket [contract qty eord & [close-orders]]
  (bracket-orders "SELL" contract qty eord  close-orders))




;; (ds/write-lines "/home/novosma/Stuff/clojure/ocs.txt" (doall (map #(vector % (oc-med (str *nas-dir* % ".csv"))) (take 10  (ds/read-lines "/home/novosma/Stuff/clojure/nas-good.txt")))))

;; (doall (map # (oc-med (str *nas-dir* % ".csv"))   (ds/read-lines "/home/novosma/Stuff/clojure/nas-good.txt")))

;; (oc-med "/home/novosma/Stuff/clojure/nas-data/ORCL.csv")

;; (ds/write-lines "/home/novosma/Stuff/clojure/nas-good.txt" 
;; 	     (doall (map  #(.getName %)
;; 	     (filter  good-st-data?
;; 		      (.listFiles (new java.io.File "/home/novosma/Stuff/clojure/nas-data/"))))))
;; (ds/write-lines "/home/novosma/Stuff/clojure/nas-good1.txt" 
;; 		(map #(second (re-find #"(.+)\.csv" %)) (read-lines "/home/novosma/Stuff/clojure/nas-good.txt")))

;(let [bars (parse-yhoo-stock-csv "/home/novosma/Stuff/clojure/nyse-data/ZZ.csv")]
  


;; (write-lines "/home/novosma/Stuff/clojure/good-syms"
;; 	     (

;(map (fn (s) ((.listFiles (new java.io.Files "/home/novosma/Stuff/clojure/nyse-data"))


;; (time (doall (vector-pmap  #(ignore-err (get-stk-file  % "/home/novosma/Stuff/clojure/nas-data")) (vec  (ds/read-lines "/home/novosma/Stuff/clojure/nasdaq-syms-cleared.txt")))))
	   
; (map #(calendar (.lastModified %)) (take 10 (.listFiles  (new File "/home/novosma/Stuff/clojure/nyse-data"))))
	  
	
;; (doall (.delete (filter #(= 22 (.get (calendar (.lastModified %)) Calendar/DATE))  (.listFiles  (new File "/home/novosma/Stuff/clojure/nyse-data")))))
;; (vector-pmap (fn (s) (ignore-err (get-stk-file s "/home/novosma/Stuff/clojure/nyse-data"))) (vec (read-line "/home/novosma/Stuff/clojure/nyse-syms-cleared.txt")))
