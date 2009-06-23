;(add-classpath "file:///C:/MySoft/clojure/clojure-contrib.jar")
(add-classpath "file:///home/novosma/Stuff/jtsapi/IBJts/dst/ib_tests.jar")
(add-classpath "file:///home/novosma/Stuff/jtsapi/IBJts/jtsclient.jar")


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

(def *good-syms* (vec (map  str 
'[ZQK YUM XTO XRX XRAY XOM XL X WYE WY WWY WSM WPS WPI WOR WMT WMS WMI WMB WM
	WLT WHR WGR WG WFT WFR WFC WEN WDC WB WAT WAG VZ VTS VPI VOD VNO VMC VLO VLCCF
	VFC VAR UVN UTX UST USG USB URI UNP UNM UNH UNA UN UHS UBB TYC TXU TXT TXN TWX
	TV TSO TSM TSG TSA TROW TRI TRB TOT TOM TOL TNE TMX TMO TMA TM TLM TK TJX TIN
	TIF TIE THX TGT TER TE TDW TBL TAP TALX T SYY SYMC SYK SWY SWN SWFT SVU SUN
	SUG SU STZ STT STRA STR STN STLD STJ STI STBA STA SSP SSCC SRE SRCL SPW SPSS
	SPLS SPG SPF SOV SO SNV SNPS SNIC SNE SNDK SLM SLE SLB SKYW SKS SIVBE SII SIGI
	SIE SIAL SHW SHPGY SHFL SGY SGP SGMS SFY SFA SEPR SEIC SEE SEBL SCSS SCHW SBUX
	SBL SBAC SAP SAFC S RYL RYAAY RX RTN RSH RSG RS RRC ROST ROK ROH RMD RMBS RL
	RIMM RIG RHI RGLD RF REGN RE RDN RDC RCL RBK RBAK RAI RA QSII QQQQ QLGC QCOM
	PZZA PXD PX PWAV PVH PTRY PTEN PSUN PSTI PSS PSA PRX PPP PPL PPG PPDI POT PNW
	PNRA PNR PNC PMI PLLL PLD PLCM PLCE PKX PKI PIXR PHM PHG PH PGR PGN PG PFE
	PFCB PETM PEP PEG PDX PDLI PDG PDE PDCO PD PCZ PCU PCP PCL PCH PCG PCAR PBI
	PBG PAYX PAAS OXY OSIP OSI OSG OS ORLY ORI ORCL OPWV ONXX OMC OKE ODP OCR NYT
	NYB NXY NWS NWRE NWL NVS NVLS NVDA NUE NU NTRS NTAP NSM NSC NOVN NOV NOK NOC
	NKE NICE NI NGAS NFX NFI NFB NEW NEM NE NDE NCX NCR NCC NBR NBL NBIX NAV NATI
	N MYL MXIM MVL MVK MUR MU MTH MTG MT MSTR MSFT MSCC MRO MRK MOT MOGN MO MNT
	MNST MNI MMM MMC MLS MLNM MLM MLHR MIK MI MHP MHO MHK MGM MFE MERQE MER MEL
	MEE MEDX MEDI MDT MDR MDG MDC MCK MCHP MCD MBI MATK MAT MAS MAR LYO LXK LUV
	LUK LTR LTD LSS LRY LRCX LPX LPNT LOW LNCR LNC LMT LM LLY LLTC LLL LIZ LIN LH
	LEN LEH LEA LDG LAMR KWK KSS KSE KRI KRB KR KOSP KO KMX KMP KMI KMG KMB KLAC
	KIM KEY KEP KCS KBH K JWN JPM JOSB JOE JNY JNPR JNJ JNC JLG JEC JCP JCI JBL
	JBHT JAH IVGN ITW ITT ISCA IRF IR IPCR IP INTU INTC INGR INFY IMDC IMCL IM IIF
	IGT IFN IDTI ICOS ICBC IBM IACI HYSL HUM HSY HSC HRS HRB HPQ HP HOV HOT HON
	HOLX HOC HNZ HNT HMT HMA HLT HIG HET HD HCBK HCA HBC HBAN HAR HANS HAL HAIN
	GYI GXP GWW GTRC GTK GT GSK GSF GS GR GPS GNTX GNSS GM GLW GLG GLBL GIS GILD
	GGP GGC GG GFI GENZ GE GDW GDT GD GCI FWLT FTO FST FSH FRX FRE FPL FOSL FO FNM
	FNF FMX FLSH FLEX FL FITB FISV FHN FFIV FE FDX FDO FDC FD FCX FBR FBN FAST
	EXPD EXP EXC EXBD EWJ ETR ETN ETH ET ESV ESS ESRX ESI ERTS ERICY EQT EQR EPEX
	EPD EP EOP EOG ENER EMR EMN EMC ELX ELN EL EK EIX EFX EDS EDMC EDE ED ECL EC
	EBAY EAT EAS E DVN DUK DTE DST DRQ DRIV DRI DPTR DOX DOW DOV DO DNB DNA DLX
	DLTR DJ DISH DIS DHR DHI DGX DG DEO DELL DE DDS DDR DD DCX DBRN DBD DB D CYMI
	CXW CWTR CVX CVTX CVS CVH CVG CVC CTXS CTX CTSH CTHR CTAS CSX CSGS CSG CSCO
	CSC CREE CRDN CPWM CPS CPRT CPB COST COP COO COGN COG COF COCO CNX CNT CNL CNI
	CNF CNB CMX CMVT CMTL CMS CMI CMCSK CMCSA CMC CMA CLX CLF CL CKFR CIN CIB CI
	CHS CHRW CHKP CHK CHIR CFFN CFC CERN CEPH CEM CELG CEG CECO CDWC CDNS CDIS CD
	CCU CCL CCK CCJ CCI CC CBT CBST CBRL CBH CBE CB CAT CAM CAL CAKE CAI CAH CA C
	BZH BZF BYD BXP BWA BUD BSX BSC BRO BRCM BR BPT BPOP BP BOW BOOM BOL BOBJ BNI
	BMY BMHC BMET BMC BLS BLL BK BJS BJ BIIB BHP BHI BER BEN BEC BEAV BDX BDK BCRX
	BCR BCO BC BBY BBT BBBY BAX BAC BA AZO AZN AYE AXP AXL AXE AVT AVP AVID AVB AU
	ATYT ATW ATVI ATMI AT ASO ASN ASML ASH ASD ARTC ARI APSG APPB APOL APD APCC
	APC APA AOS AOC ANN ANF AMZN AMTD AMT AMR AMLN AMGN AMG AMED AMD AMB AMAT AM
	ALV ALTR ALL ALKS ALK AL AIV AIG AHC AGN AFL AFG AFFX AF AET AES AEP AEOS AEM
	AEE ADTN ADSK ADRX ADP ADM ADI ADCT ADBE ACXM ACV ACS ACI ACF ACE ACAS AC ABX
	ABT ABS ABK ABI ABGX ABFS ABC AAUK AAPL AAI AA])))

(defn parse-date [dt]
  (cond 
   (string? dt) (.parse *df* dt)
   (number? dt) (new Date (long dt))
   true    dt))

(defn calendar [& [dt]]
  (let [c (Calendar/getInstance)]
  (.setTime c (if dt (parse-date dt) (new Date))) c))

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
        agents (split-for-n v n)]
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



(defn oc-med [file]
  (let [ocs
	(doall (map #(let [arange (- (:high %)  (:low %))] (if (> arange 0)  (Math/abs (/ (- (:close %) (:open %)) arange)) 0))
		    (parse-yhoo-stock-csv file)))]
    (median ocs)))

(def *nas-dir* "/home/novosma/Stuff/clojure/nas-data/")

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

(def *eclient* (new EClientSocket 
		    (proxy [ApiCallback] []
			(historicalData [reqId date open high low close volume count WAP hasGaps ]
					( on-hist-data reqId date open high low close volume count WAP hasGaps ))
			(error  [id  errorCode errorString]
				(on-error id  errorCode errorString)))))
				


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

(defn get-hist-data [contract & [endDateTime durationStr]]
  (let [req-id (get-id)
	end-date (if endDateTime (parse-date endDateTime) 
		     (.format (new SimpleDateFormat "yyyyMMdd hh:mm:ss") (new Date)))]
    (sync-bridge req-id 
		 (fn []
		   (.reqHistoricalData *eclient* 
				       req-id  contract end-date  
				       "2 D" "1 day" (if (= (.m_secType contract) "STK") "TRADES" "MIDPOINT")
							    1 1)))))
  
  


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
