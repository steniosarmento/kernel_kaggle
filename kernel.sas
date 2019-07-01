***********************************************************************************************;
* -                  Competição DSA de Machine Learning - Edição Junho/2019                 - *;
* =========================================================================================== *;
* Este kernel prevê um índice de lealdade para cada card_id representado em dataset_teste.csv *;
* e sample_submission.csv.                                                                    *;
* Link da competição:                                                                         *;
*        https://www.kaggle.com/c/competicao-dsa-machine-learning-jun-2019/                   *;
***********************************************************************************************;
* Author  : Stenio Sarmento de Assis                                                          *;
* Date    : 2019-06-26                                                                        *;
* Runtime : 15 minutos                                                                        *;
* Linkedin: https://br.linkedin.com/in/stenio-sarmento                                        *;
***********************************************************************************************;

OPTIONS NOSOURCE NONOTES;

LIBNAME BASES "/dados/gaudit/demandas/2019/GA Eletronica/kaggle";

%MACRO UNIFICA_TRANSACOES;
   %put .    -> UNIFICA_TRANSACOES  : %sysfunc(time(),time12.3);
   PROC SQL;
      CREATE TABLE BASES.TRANSACOES AS 
         SELECT
            *
         FROM
            (SELECT
                T1.*
             FROM
                BASES.NOVAS_TRANSACOES_COMER T1
             UNION
             SELECT
                T2.*
             FROM
               BASES.TRANSACOES_HISTORICAS T2)
         ORDER BY
            CARD_ID,
            PURCHASE_DATE;
   QUIT;
%MEND;

%MACRO REMOVE_COM_DUPL;
   %put .    -> REMOVE_COM_DUPL     : %sysfunc(time(),time12.3);
   DATA BASES.COMERCIANTES_UNICOS;
      SET BASES.COMERCIANTES;
      BY MERCHANT_ID;
      IF FIRST.MERCHANT_ID THEN
         OUTPUT;
   RUN;
%MEND;

%MACRO GERA_BASES;
   %put .    -> GERA_BASES          : %sysfunc(time(),time12.3);
   PROC SQL;
      CREATE TABLE BASES.TEMP_TREINO AS
         SELECT
            H.AUTHORIZED_FLAG,
            CASE (H.AUTHORIZED_FLAG)
               WHEN ("N") THEN 1
               WHEN ("Y") THEN 2
               ELSE 3
            END AS authorized_flag_n,
            H.CARD_ID,
            H.CITY_ID,
            H.CATEGORY_1,
            CASE (H.CATEGORY_1)
               WHEN ("N") THEN 1
               WHEN ("Y") THEN 2
               ELSE 3
            END AS category_1_n,
            H.INSTALLMENTS,
            H.CATEGORY_3,
            CASE (H.CATEGORY_3)
               WHEN ("A") THEN 3
               WHEN ("B") THEN 2
               WHEN ("C") THEN 1
               ELSE 0
            END AS category_3_n,
            H.MERCHANT_CATEGORY_ID,
            H.MERCHANT_ID,
            CASE (H.MONTH_LAG)
               WHEN (0) THEN 0
               WHEN (1) THEN 1
               WHEN (2) THEN 2
               ELSE 3
            END AS month_lag,
            H.PURCHASE_AMOUNT,
            H.PURCHASE_DATE,
            H.CATEGORY_2,
            H.STATE_ID,
            H.SUBSECTOR_ID,
            C.MERCHANT_GROUP_ID,
            C.NUMERICAL_1,
            C.NUMERICAL_2,
            C.MOST_RECENT_SALES_RANGE,
            CASE (C.MOST_RECENT_SALES_RANGE)
               WHEN ("A") THEN 5
               WHEN ("B") THEN 4
               WHEN ("C") THEN 3
               WHEN ("D") THEN 2
               WHEN ("E") THEN 1
               ELSE 0
            END AS most_recent_sales_range_n,
            C.MOST_RECENT_PURCHASES_RANGE,
            CASE (C.MOST_RECENT_PURCHASES_RANGE)
               WHEN ("A") THEN 5
               WHEN ("B") THEN 4
               WHEN ("C") THEN 3
               WHEN ("D") THEN 2
               WHEN ("E") THEN 1
               ELSE 0
            END AS most_recent_purchases_range_n,
            C.AVG_SALES_LAG3,
            C.AVG_PURCHASES_LAG3,
            INPUT(C.AVG_PURCHASES_LAG3,BEST12.) AS avg_purchases_lag3_n,
            C.ACTIVE_MONTHS_LAG3,
            C.AVG_SALES_LAG6,
            C.AVG_PURCHASES_LAG6,
            INPUT(C.AVG_PURCHASES_LAG6,BEST12.) AS avg_purchases_lag6_n,
            C.ACTIVE_MONTHS_LAG6,
            C.AVG_SALES_LAG12,
            C.AVG_PURCHASES_LAG12,
            INPUT(C.AVG_PURCHASES_LAG12,BEST12.) AS avg_purchases_lag12_n,
            C.ACTIVE_MONTHS_LAG12,
            C.CATEGORY_4,
            CASE (C.CATEGORY_4)
              WHEN ("N") THEN 1
              WHEN ("Y") THEN 2
              ELSE 0
            END AS category_4_n,
            T.FIRST_ACTIVE_MONTH,
            T.FEATURE_1,
            T.FEATURE_2,
            T.FEATURE_3,
            T.TARGET
         FROM
            BASES.TRANSACOES          H INNER JOIN
            BASES.DATASET_TREINO      T ON (H.CARD_ID     = T.CARD_ID)LEFT JOIN
            BASES.COMERCIANTES_UNICOS C ON (H.MERCHANT_ID = C.MERCHANT_ID)
         ORDER BY
            H.CARD_ID ASC,
            H.PURCHASE_DATE ASC;
   QUIT;

   PROC SQL;
      CREATE TABLE BASES.TEMP_TESTE AS
         SELECT
            H.AUTHORIZED_FLAG,
            CASE (H.AUTHORIZED_FLAG)
               WHEN ("N") THEN 1
               WHEN ("Y") THEN 2
               ELSE 3
            END AS authorized_flag_n,
            H.CARD_ID,
            H.CITY_ID,
            H.CATEGORY_1,
            CASE (H.CATEGORY_1)
               WHEN ("N") THEN 1
               WHEN ("Y") THEN 2
               ELSE 3
            END AS category_1_n,
            H.INSTALLMENTS,
            H.CATEGORY_3,
            CASE (H.CATEGORY_3)
               WHEN ("A") THEN 3
               WHEN ("B") THEN 2
               WHEN ("C") THEN 1
               ELSE 0
            END AS category_3_n,
            H.MERCHANT_CATEGORY_ID,
            H.MERCHANT_ID,
            CASE (H.MONTH_LAG)
               WHEN (0) THEN 0
               WHEN (1) THEN 1
               WHEN (2) THEN 2
               ELSE 3
            END AS month_lag,
            H.PURCHASE_AMOUNT,
            H.PURCHASE_DATE,
            H.CATEGORY_2,
            H.STATE_ID,
            H.SUBSECTOR_ID,
            C.MERCHANT_GROUP_ID,
            C.NUMERICAL_1,
            C.NUMERICAL_2,
            C.MOST_RECENT_SALES_RANGE,
            CASE (C.MOST_RECENT_SALES_RANGE)
               WHEN ("A") THEN 5
               WHEN ("B") THEN 4
               WHEN ("C") THEN 3
               WHEN ("D") THEN 2
               WHEN ("E") THEN 1
               ELSE 0
            END AS most_recent_sales_range_n,
            C.MOST_RECENT_PURCHASES_RANGE,
            CASE (C.MOST_RECENT_PURCHASES_RANGE)
               WHEN ("A") THEN 5
               WHEN ("B") THEN 4
               WHEN ("C") THEN 3
               WHEN ("D") THEN 2
               WHEN ("E") THEN 1
               ELSE 0
            END AS most_recent_purchases_range_n,
            C.AVG_SALES_LAG3,
            C.AVG_PURCHASES_LAG3,
            INPUT(C.AVG_PURCHASES_LAG3,BEST12.) AS avg_purchases_lag3_n,
            C.ACTIVE_MONTHS_LAG3,
            C.AVG_SALES_LAG6,
            C.AVG_PURCHASES_LAG6,
            INPUT(C.AVG_PURCHASES_LAG6,BEST12.) AS avg_purchases_lag6_n,
            C.ACTIVE_MONTHS_LAG6,
            C.AVG_SALES_LAG12,
            C.AVG_PURCHASES_LAG12,
            INPUT(C.AVG_PURCHASES_LAG12,BEST12.) AS avg_purchases_lag12_n,
            C.ACTIVE_MONTHS_LAG12,
            C.CATEGORY_4,
            CASE (C.CATEGORY_4)
              WHEN ("N") THEN 1
              WHEN ("Y") THEN 2
              ELSE 0
            END AS category_4_n,
            T.FIRST_ACTIVE_MONTH,
            T.FEATURE_1,
            T.FEATURE_2,
            T.FEATURE_3
         FROM
            BASES.TRANSACOES          H INNER JOIN
            BASES.DATASET_TESTE       T ON (H.CARD_ID     = T.CARD_ID)LEFT JOIN
            BASES.COMERCIANTES_UNICOS C ON (H.MERCHANT_ID = C.MERCHANT_ID)
         ORDER BY
            H.CARD_ID ASC,
            H.PURCHASE_DATE ASC;
   QUIT;
%MEND;

%MACRO REGRESSAO_LINEAR;
   %put .    -> REGRESSAO_LINEAR    : %sysfunc(time(),time12.3);

   %_eg_conditional_dropds(WORK.PREDICOES,
         WORK.TMP0TempTableAddnlPredictData,
         WORK.SORTTempTableSorted,
         WORK.TMP1TempTableForPlots);

   DATA WORK.TMP0TempTableAddnlPredictData;
      SET BASES.TEMP_TREINO(IN=__ORIG) BASES.TEMP_TESTE;
      __FLAG=__ORIG;
      __DEP=target;
      if not __FLAG then target=.;
   RUN;

   DATA _NULL_;
      dsid = OPEN("WORK.TMP0TempTableAddnlPredictData", "I");
      dstype = ATTRC(DSID, "TYPE");
      IF TRIM(dstype) = " " THEN
         DO;
         CALL SYMPUT("_EG_DSTYPE_", "");
         CALL SYMPUT("_DSTYPE_VARS_", "");
         END;
      ELSE
         DO;
         CALL SYMPUT("_EG_DSTYPE_", "(TYPE=""" || TRIM(dstype) || """)");
         IF VARNUM(dsid, "_NAME_") NE 0 AND VARNUM(dsid, "_TYPE_") NE 0 THEN
            CALL SYMPUT("_DSTYPE_VARS_", "_TYPE_ _NAME_");
         ELSE IF VARNUM(dsid, "_TYPE_") NE 0 THEN
            CALL SYMPUT("_DSTYPE_VARS_", "_TYPE_");
         ELSE IF VARNUM(dsid, "_NAME_") NE 0 THEN
            CALL SYMPUT("_DSTYPE_VARS_", "_NAME_");
         ELSE
            CALL SYMPUT("_DSTYPE_VARS_", "");
         END;
      rc = CLOSE(dsid);
      STOP;
   RUN;

   DATA WORK.SORTTempTableSorted &_EG_DSTYPE_ / VIEW=WORK.SORTTempTableSorted;
      SET WORK.TMP0TempTableAddnlPredictData;
   RUN;

   PROC REG DATA=WORK.SORTTempTableSorted
      ;
      Linear_Regression_Model: MODEL target = authorized_flag_n city_id category_1_n installments category_3_n merchant_category_id month_lag purchase_amount purchase_date category_2 state_id subsector_id merchant_group_id numerical_1 numerical_2 most_recent_sales_range_n most_recent_purchases_range_n avg_sales_lag3 avg_purchases_lag3_n active_months_lag3 avg_sales_lag6 avg_purchases_lag6_n active_months_lag6 avg_sales_lag12 avg_purchases_lag12_n active_months_lag12 category_4_n feature_1 feature_2 feature_3
         /      SELECTION=STEPWISE
         SLE=0.05
         SLS=0.05
         INCLUDE=0
      ;

      OUTPUT OUT=WORK.PREDICOES(LABEL="Linear regression predictions and statistics for BASES.TEMP_TREINO" WHERE=(NOT __FLAG))
         PREDICTED=predicted_target ;
   RUN;
   QUIT;

   DATA WORK.PREDICOES; 
      set WORK.PREDICOES; 
      target=__DEP; 
      DROP __DEP; 
      DROP __FLAG;
   RUN; 

   RUN; QUIT;

   %_eg_conditional_dropds(WORK.TMP0TempTableAddnlPredictData,
         WORK.SORTTempTableSorted,
         WORK.TMP1TempTableForPlots);

%MEND;

%MACRO GERA_RESULTADO;
   %put .    -> GERA_RESULTADO      : %sysfunc(time(),time12.3);
   PROC SQL;
      CREATE TABLE BASES.SUBMISSION AS 
         SELECT
            T3.CARD_ID, 
            ROUND((MEAN(T3.TARGET)),0.00000001) FORMAT=BEST12. INFORMAT=BEST12. AS target
         FROM
           (SELECT
               T1.CARD_ID,
               COALESCE(T1.PREDICTED_TARGET, (SELECT
                                                (MEAN(T2.PREDICTED_TARGET)) AS MEDIA
                                              FROM
                                                 WORK.PREDICOES T2
                                              WHERE T2.PREDICTED_TARGET NOT IS MISSING)) AS TARGET
         FROM WORK.PREDICOES T1) T3
         GROUP BY T3.CARD_ID
         ORDER BY T3.CARD_ID;
   QUIT;
%MEND;

%MACRO DELETA_TEMP;
   %put .   -> DELETA_TEMP: %sysfunc(time(),time12.3);
   PROC DATASETS LIBRARY=BASES NOLIST;
      DELETE TRANSACOES;
      DELETE COMERCIANTES_UNICOS;
      DELETE TEMP_TREINO;
      DELETE TEMP_TESTE;
   RUN;
   PROC DATASETS LIBRARY=WORK NOLIST;
      DELETE PREDICOES;
   RUN;
%MEND;

* =========================================================================================== *;
* -                                             MAIN                                        - *;
* =========================================================================================== *;
%put . -> INÍCIO do processamento: %sysfunc(time(),time12.3);
%UNIFICA_TRANSACOES;
%REMOVE_COM_DUPL;
%GERA_BASES;
%REGRESSAO_LINEAR;
%GERA_RESULTADO;
%DELETA_TEMP;
%put . -> FIM    do processamento: %sysfunc(time(),time12.3);